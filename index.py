import threading
import time

number_of_messages = 0
all_threads = []

class Queue:
    
    def __init__(self):
        self.topic_processors = dict()
        self.counter = 1001
        
    def create_topic(self, topic_name):
        topic_id = self.counter
        self.counter += 1
        topic = Topic(topic_name,topic_id)
        topic_handler = TopicHandler(topic)
        self.topic_processors[topic_id] = topic_handler
        
    def subscribe(self, topic_id, subscriber):
        self.topic_processors[topic_id].topic.add_subscriber(subscriber)
        # print(f'Queue:: {topic_id}:: subscriber "{subscriber.id}" is subscriber to topic {topic_id}')
        
    def publish(self, topic_id, message):
        self.topic_processors[topic_id].topic.add_message(message)
        print(f'Queue:: {topic_id}:: PUBLISH MESSAGE => {message}')
        publish_thread = threading.Thread(target=self.topic_processors[topic_id].publish)
        publish_thread.start()
        global all_threads
        all_threads.append(publish_thread)
        # self.topic_processors[topic_id].publish()
        
    def print_topics(self):
        for topic_uuid, topic_handler in self.topic_processors.items():
            print()
            print('##########')
            print('NAME:',topic_handler.topic.name)
            print('ID:',topic_uuid)
            print('MESSAGES:',topic_handler.topic.messages)
            print('SUBS:',topic_handler.topic.subscribers)
            print('##########')
        
class TopicHandler:
    
    def __init__(self, topic):
        self.topic = topic  
        self.workers = dict()
        
    def publish(self):
        # print(f'TopicHandler:: {self.topic.id}:: time to call all the subscribers...')
        for subscriber in self.topic.subscribers: 
            self.start_subcriber_workers(subscriber)
            
    def start_subcriber_workers(self, subscriber):
        if subscriber.id not in self.workers:
            subscriber_worker = SubscriberWorker(self.topic, subscriber)
            worker_thread = threading.Thread(target=subscriber_worker.run)
            worker_thread.start()
            global all_threads
            all_threads.append(worker_thread)
        
        
        
class SubscriberWorker:
    
    def __init__(self, topic, subscriber):
        self.topic = topic
        self.subscriber = subscriber
        
    def run(self):
        print(f'SubscriberWorker:: For topic {self.topic.id}, running the worker for subscriber {self.subscriber.id}')
        while self.subscriber.offset < len(self.topic.messages):  
            with self.subscriber.lock:
                if(self.subscriber.offset < len(self.topic.messages)):
                    global number_of_messages 
                    number_of_messages += 1
                    curOffset = self.subscriber.offset
                    message = self.topic.messages[curOffset]
                    self.subscriber.consume(message)
                    self.subscriber.offset += 1
                else:
                    print(f'Damm someone else did...{threading.currentThread()}')
                
class Topic:
    
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.messages = []
        self.subscribers = []
    
    def add_message(self, message):
        self.messages.append(message)

    def add_subscriber(self, subscriber):
        self.subscribers.append(subscriber)
        
class Subscriber:
    
    def __init__(self, id):
        self.id = id
        self.offset = 0
        self.condition = threading.Condition()
        self.lock = threading.Lock()
        
    def consume(self, message):
        print(f'Subscriber:: {self.id}:: got message=> {message} => {threading.currentThread()}')

    def notify_new_message(self):
        with self.condition:
            self.condition.notify()        
                
if __name__ == "__main__":
    
    queue = Queue()
    queue.create_topic('First Topic')
    queue.create_topic('Second Topic')
    chat_subscriber = Subscriber("CHAT-HANDLER")
    push_subscriber = Subscriber("PUSH-HANDLER")
    sms_subscriber = Subscriber("SMS-HANDLER")
    
    for i in range(1,21):
        queue.publish(1001, f"MESSAGE {i}")
        if i == 1:
            queue.subscribe(1001, push_subscriber)
        if i == 5:
            queue.subscribe(1001, sms_subscriber)
        if i == 15:
            queue.subscribe(1001,chat_subscriber)
     
    print("Waiting for threads to finish...")
    print("Number of thread created:", len(all_threads))
    for each in all_threads:
        # print(each)
        each.join()       
    print("Finished waiting...")
    
    print('Number of messages printed', number_of_messages)
