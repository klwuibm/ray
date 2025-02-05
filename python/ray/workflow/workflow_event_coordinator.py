import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import asyncio
import threading

import ray
from ray.workflow import common, recovery, step_executor, storage, workflow_storage, workflow_access
from ray.util.annotations import PublicAPI
from ray.workflow.event_listener import EventListener, EventListenerType, TimerListener
from ray.workflow.workflow_access import (
    flatten_workflow_output,
    get_or_create_management_actor,
    get_management_actor,
)
from ray.workflow.storage import Storage, get_global_storage
from ray.workflow.workflow_storage import WorkflowStorage
from ray.workflow import serialization
from ray.workflow import serialization_context

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)

@ray.remote(num_cpus=0)
class EventCoordinatorActor:
    """Track event steps and checkpoint arrived data before resuming a workflow """

    def __init__(self, wma: "workflow_access.WorkflowManagementActor"):
        import nest_asyncio
        nest_asyncio.apply()

        self.wma = wma
        self.store_url = ray.get(self.wma.get_storage_url.remote())
        self.event_registry: Dict[str, Dict[str, asyncio.Future]] = {}
        self._num_ownership_transferred: Dict[str, int] = {}
        self.transfer_and_event_arrival_lock = asyncio.Lock()
        self.resume_lock = asyncio.Lock()
        self._num_event_received: Dict[str, int] = {}
        self._time_to_resume: Dict[str, float] = {}

    async def poll_event_checkpoint_then_resume \
        (self, workflow_id:str, current_step_id:str, outer_most_step_id:str, \
        event_listener_type:EventListener, *args, **kwargs) -> Tuple[str, str]:
        """Poll event listener; checkpoint event; resume the workflow """

        event_listener = event_listener_type()
        event_content = await event_listener.poll_for_event(*args, **kwargs)
        await self.checkpointEvent(workflow_id, current_step_id, outer_most_step_id, event_content)
        logger.info(f"ECA received an event")

        async with self.transfer_and_event_arrival_lock:
            if workflow_id not in self._num_event_received.keys():
                self._num_event_received[workflow_id] = 1
            else:
                self._num_event_received[workflow_id] += 1
        # if self.resume_lock is held by someone else, simply returns
        if self.resume_lock.locked():
            return(workflow_id, current_step_id)
        # check if a wma.run_or_resume() should be called
        async with self.resume_lock:
            if workflow_id not in self._time_to_resume.keys():
                self._time_to_resume[workflow_id] = time.time()+0.5
            fire_resume = False
            if time.time() > self._time_to_resume[workflow_id]:
                try:
                    root_status = ray.get(self.wma.get_workflow_root_status.remote(workflow_id))
                except KeyError:
                    logger.info(f"ECA could not get root_status from wma: {workflow_id} KeyError")
                    pass
                # logger.info(f"ECA: root_status: {root_status}")
                status, _ = step_executor._resolve_object_ref(root_status)
                if status == common.WorkflowStatus.SUSPENDED:
                    fire_resume = True
            elif self._num_event_received[workflow_id] == self._num_ownership_transferred[workflow_id]:
                # the last event has been received
                await asyncio.sleep(self._time_to_resume[workflow_id]-time.time())
                fire_resume = True
            if fire_resume:
                logger.info(f"ECA launch run_or_resume")
                ray.get(self.wma.run_or_resume.remote(workflow_id, ignore_existing = True, is_eca_initiated = True))
                self._time_to_resume[workflow_id] = time.time() + 0.5

        return (workflow_id, current_step_id)

    async def transferEventStepOwnership(self, \
        workflow_id:str, current_step_id:str, outer_most_step_id:str, \
        event_listener_type:EventListener, *args, **kwargs) -> str:
        """Transfer one event step from workflow execution to event coordinator.

        Args:
            workflow_id: The ID of the workflow.
            current_step_id: The ID of the current event step.
            outer_most_step_id: The ID of the outer most step.
            event_listener_type: The event listener class used to connect to the event provider.
            *args, **kwargs: Optional parameters, such as connection info, to be passed to the event listener.

        Returns:
            "REGISTERED"
        """

        async with self.transfer_and_event_arrival_lock:
            if workflow_id not in self._num_ownership_transferred.keys():
                self._num_ownership_transferred[workflow_id] = 1
            else:
                self._num_ownership_transferred[workflow_id] += 1
            if workflow_id not in self.event_registry:
                self.event_registry[workflow_id] = {}
            if current_step_id not in self.event_registry[workflow_id]:
                # self.event_registry[workflow_id][current_step_id].cancel()
                # self.event_registry[workflow_id].pop(current_step_id)
                self.event_registry[workflow_id][current_step_id] = asyncio.ensure_future( \
                self.poll_event_checkpoint_then_resume(workflow_id, current_step_id, outer_most_step_id, \
                event_listener_type, *args, **kwargs))

        return "REGISTERED"

    async def checkpointEvent(self, \
        workflow_id:str, current_step_id:str, outer_most_step_id:str, content:Any) -> None:
        """Save received event content to workflow checkpoint """

        ws = WorkflowStorage(workflow_id, storage.create_storage(self.store_url))
        ws.save_step_output(
            current_step_id, content, exception=None, outer_most_step_id=outer_most_step_id
        )

    async def cancelWorkflowListeners(self, workflow_id:str) -> str:
        """Cancel all event listeners associated with the workflow.
        Args:
            workflow_id: The ID of the workflow.

        Returns:
            "CANCELLED"
        """

        async with self.write_lock:
            if workflow_id in self.event_registry.keys():
                listeners = self.event_registry[workflow_id]
                for v in listeners.values():
                    if not v.done():
                        v.cancel()
            self.event_registry.pop(workflow_id)
        return "CANCELED"

def init_event_coordinator_actor() -> None:
    """Initialize EventCoordinatorActor"""

    workflow_manager = get_management_actor()
    event_coordinator = EventCoordinatorActor.options(
        name=common.EVENT_COORDINATOR_NAME,
        namespace=common.EVENT_COORDINATOR_NAMESPACE,
        lifetime="detached",
    ).remote(workflow_manager)
    actor_handle = ray.get_actor(common.EVENT_COORDINATOR_NAME, namespace=common.EVENT_COORDINATOR_NAMESPACE)

def get_event_coordinator_actor() -> "ActorHandle":
    return ray.get_actor(
        common.EVENT_COORDINATOR_NAME, namespace=common.EVENT_COORDINATOR_NAMESPACE
    )

def get_or_create_event_coordinator_actor() -> "ActorHandle":
    """Get or create EventCoordinatorActor"""

    try:
        event_coordinator = get_event_coordinator_actor()
    except ValueError:
        store = storage.get_global_storage()
        # the actor does not exist
        logger.warning(
            "Cannot access event coordinator. It could be because "
            "the event coordinator exited unexpectedly. A new "
            "event coordinator is being created. "
        )
        event_coordinator = EventCoordinatorActor.options(
            name=common.EVENT_COORDINATOR_NAME,
            namespace=common.EVENT_COORDINATOR_NAMESPACE,
            lifetime="detached",
        ).remote(get_management_actor())
    return event_coordinator
