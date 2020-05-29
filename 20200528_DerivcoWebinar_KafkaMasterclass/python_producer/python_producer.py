from confluent_kafka import Producer
from faker import Faker
from faker.providers import profile
from faker.providers import python
import json
import datetime
import time

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to T[{}] P[{}] O[{}]'.format(msg.topic(), msg.partition(), msg.offset()))

def generate_players(count):
    players = []
    for _ in range(count):
        player = {}
        player['profile'] = fake.simple_profile()
        player['morality'] = fake.pyint(1,30)
        player['balance'] = 0.00
        players.append(player)
    return players

def send_player_deposit(player, event_ts, deposit_amount):
    data = {}
    data['type'] = 'deposit'
    data['username'] = player['profile']['username']
    data['profile'] = player['profile']
    data['event_ts'] = event_ts
    data['deposit_amount'] = deposit_amount
    producer.produce('DEPOSITS', key=data['username'], value=json.dumps(data, indent=4, sort_keys=True, default=str), callback=delivery_report)

def send_player_wager(player, event_ts, bet_amount, win_amount, start_balance_amount, end_balance_amount):
    data = {}
    data['type'] = 'wager'
    data['username'] = player['profile']['username']
    data['profile'] = player['profile']
    data['event_ts'] = event_ts
    data['bet_amount'] = bet_amount
    data['win_amount'] = win_amount
    data['start_balance_amount'] = start_balance_amount
    data['end_balance_amount'] = end_balance_amount
    producer.produce('WAGERS', key=data['username'], value=json.dumps(data, indent=4, sort_keys=True, default=str), callback=delivery_report)

def wager_players(event_ts):
    for player in players:
        bet_amount = fake.pyint(50,5000)
        win_amount = fake.pyint(0,180)
        win_multiplier = fake.pyint(0,100)

        if win_multiplier > 95:
            win_amount = win_amount * 50
        elif win_multiplier > 85:
            win_amount = win_amount * 10

        if player['morality'] > 3:
            if player['balance'] == 0:
                deposit_amount = fake.pyint(500, 5000, 100)
                player['balance'] =  player['balance'] + deposit_amount
                send_player_deposit(player, event_ts, deposit_amount)
            elif bet_amount > player['balance']:
                bet_amount = player['balance']
                end_balance = player['balance'] - bet_amount + win_amount
                send_player_wager(player, event_ts, bet_amount, win_amount, player['balance'], end_balance)
                player['balance'] = end_balance
            elif bet_amount <= player['balance']:
                end_balance = player['balance'] - bet_amount + win_amount
                send_player_wager(player, event_ts, bet_amount, win_amount, player['balance'], end_balance)
                player['balance'] = end_balance
        elif player['morality'] <= 3: # Bad Players
            bet_amount = 314.159
            end_balance = player['balance'] - bet_amount + win_amount
            send_player_wager(player, event_ts, bet_amount, win_amount, player['balance'], end_balance)
            player['balance'] = end_balance

producer = Producer({
    'bootstrap.servers': 'localhost:9092', 
    'linger.ms':50, 
    'batch.num.messages':1000
    })

fake = Faker()
fake.add_provider(profile)
fake.add_provider(python)

players = generate_players(100)

loopedyLoops = 200
while(loopedyLoops > 0):
    wager_players(datetime.datetime)
    time.sleep(3)
    loopedyLoops -= 1
