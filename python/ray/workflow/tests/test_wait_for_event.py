import ray
from ray import workflow
from ray.workflow.event_listener import EventListener
#from sqs_listener import SQSEventListener
import asyncio
import time
import random

ray.init(address='auto')
workflow.init()

class ExampleEventProvider(EventListener):
    def __init__(self):
        pass

    async def poll_for_event(self, *args, **kwargs):
        await asyncio.sleep(random.uniform(3, 10))
        event_content = args[0]
        return event_content

    async def event_checkpointed(self, *args):
        pass

@workflow.step
def handle_event(*args):
    return args[0]

# @workflow.step
# def e1():
#     return workflow.wait_for_event_revised(ExampleEventProvider, "hello")
e1 = workflow.wait_for_event_revised(ExampleEventProvider, "e1:hello")
@workflow.step
def e2():
    return workflow.wait_for_event_revised(ExampleEventProvider, "e2.step():hello")
#e2 = workflow.wait_for_event_revised(ExampleEventProvider, "hello")
@workflow.step
def w1():
    time.sleep(20)
    return 1

@workflow.step
def w2():
    time.sleep(25)
    return 2

@workflow.step
def w3():
    return 3

async def __main__(*args, **kwargs):
    import nest_asyncio
    nest_asyncio.apply()

    res = handle_event.step([e1, e2.step(), w1.step(), w2.step()]).run(workflow_id='test_event')
    print('first time', res)
    await asyncio.sleep(40)
    res = ray.get(workflow.get_output(workflow_id='test_event'))
    print('second time', res)
    #res = handle_event.step(workflow.wait_for_event_revised.step(ExampleEventProvider, "hello")).run()
    #res = handle_event.step([e1, e2.step(), w3.step()]).run(workflow_id='test_event')
    #res = handle_event.step([w1.step(),w2.step(),w3.step()]).run()

asyncio.run(__main__())
