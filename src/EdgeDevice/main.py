import os
from time import sleep

def await_instruction():
    options = ['receive data', 'receive model', 'start training', 'stop']
    while True:
        input_data = input(f'\nwaiting for instructions:\n{options}\n')
        if input_data == 'receive data':
            receive_data()
        if input_data == 'receive model':
            receive_model()
        if input_data == 'start training':
            train_model()
        if input_data == 'stop':
            exit()

def receive_data():
    print('receiving data')
    print('validating data')
    print('replacing data')

def receive_model():
    print('receiving model')
    print('validating model')
    print('replacing model')
    train_model()

def train_model():
    print(os.listdir('../../data/'))
    if not (os.listdir('../../data/')):
        print('no training data found')
        return
    print('training model...')
    sleep(1)
    print('training finished')
    send_gradient_data()

def send_gradient_data():
    print('sending gradient data')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    while True:
        await_instruction()



