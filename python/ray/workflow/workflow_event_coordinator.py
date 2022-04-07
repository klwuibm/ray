import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import asyncio
import threading

import ray
from ray.workflow import common, recovery, storage, workflow_storage, workflow_access
from ray.util.annotations import PublicAPI
from ray.workflow.event_listener import EventListener, EventListenerType, TimerListener
from ray.workflow.workflow_access import (
    flatten_workflow_output,
    get_or_create_management_actor,
    get_management_actor,
)

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)

@ray.remote(num_cpus=0)
class EventCoordinatorActor:
    def __init__(self, wma: "workflow_access.WorkflowManagementActor"):
        self.wma = wma
        self.event_registry: Dict[str, Dict[str, asyncio.Future]] = {}
        self.write_lock = asyncio.Lock()

    async def poll_event_checkpoint_then_resume \
        (self, workflow_id, current_step_id, outer_most_step_id, \
        event_listener_handle, *args, **kwargs) -> Tuple[str, str]:

        event_listener = event_listener_handle()
        event_content = await event_listener.poll_for_event(*args, **kwargs)

        await self.checkpointEvent(workflow_id, current_step_id, outer_most_step_id, event_content)
        self.wma.run_or_resume(workflow_id)

        return (workflow_id, current_step_id)

    async def transferEventStepOwnership(self, \
        workflow_id, current_step_id, outer_most_step_id, event_listener_handle, *args, **kwargs) -> str:
        async with self.write_lock:
            self.event_registry[workflow_id][current_step_id] = asyncio.ensure_future( \
                self.poll_event_checkpoint_then_resume(workflow_id, current_step_id, outer_most_step_id, \
                event_listener_handle, *args, **kwargs))

        return "REGISTERED"

    async def checkpointEvent(self, workflow_id, current_step_id, outer_most_step_id, content) -> None:
        ws = WorkflowStorage(workflow_id, storage.create_storage(self.wma.get_storage_url()))
        ws.save_step_output(
            current_step_id, content, exception=None, outer_most_step_id=outer_most_step_id
        )

    async def cancelWorkflowListeners(self, workflow_id) -> str:
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
