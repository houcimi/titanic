import streamlit as st
from confluent_kafka import Producer, Consumer
import requests
import json
import dataikuapi

# Initialize Kafka Producer
producer_conf = {'bootstrap.servers': 'pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'NAWGVIPESF2U7ZJ2',
        'sasl.password': 'Z+dN7SAyadAMruSxrrw1EDQoPwFi+njUav0b+saInDjCH3nBtfXK2OvUdamGYy/w',
        'group.id': 'test'}


producer = Producer(producer_conf)

# Initialize Kafka Consumer
consumer_conf  = {'bootstrap.servers': 'pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'NAWGVIPESF2U7ZJ2',
        'sasl.password': 'Z+dN7SAyadAMruSxrrw1EDQoPwFi+njUav0b+saInDjCH3nBtfXK2OvUdamGYy/w',
        'group.id': 'test'}
consumer = Consumer(consumer_conf)
consumer.subscribe(['your_output_topic'])
client = dataikuapi.APINodeClient("https://api-a7bc0c16-c60e2a0a-dku.eu-west-3.app.dataiku.io/", "Titanic1")


# Function to send data to Kafka
def send_to_kafka(data):
    producer.produce('your_input_topic', value=json.dumps(data))
    producer.flush()


# Function to get prediction from Dataiku model
def get_prediction(data):
    # Remove extra fields that are not expected by the model
    if 'timestamp' in data:
        del data['timestamp']

    # Send the data to the Dataiku model and get the prediction
    try:
        prediction_result = client.predict_record("titanic", data)
        return prediction_result["result"]
    except Exception as e:
        st.error(f"Error obtaining prediction: {str(e)}")
        return None


# Streamlit interface
st.title('Data Processing with Kafka and Dataiku')

if st.button('Send Data'):
    # Simulate data fetching
    data = {'key1': 'value1', 'key2': 'value2'}
    send_to_kafka(data)
    st.write('Data sent to Kafka!')

# Consume data, get prediction, and display
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break
    if msg.error():
        st.write(f"Consumer error: {msg.error()}")
        break

    received_message = json.loads(msg.value().decode('utf-8'))
    # Assuming 'response' contains the data for prediction
    if 'response' in received_message:
        prediction_data = received_message['response']
        prediction = get_prediction(prediction_data)

        # Display prediction
        if prediction is not None:
            st.write(f"Prediction: {prediction}")

consumer.close()
