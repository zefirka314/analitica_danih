import time
import random
import psycopg2
from datetime import datetime, timedelta
import os
import logging
import uuid

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GameDataGenerator:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'game_stats_db'),
            'user': os.getenv('DB_USER', 'game_user'),
            'password': os.getenv('DB_PASSWORD', 'game_pass'),
            'port': 5432
        }
        
        # Игроки и их текущие уровни
        self.players = [
            'Player_' + str(i) for i in range(1, 21)  # 20 игроков
        ]
        
        self.player_levels = {player: random.randint(1, 50) for player in self.players}
        self.player_scores = {player: random.randint(100, 50000) for player in self.players}
        
        # Типы игровых событий
        self.game_events = [
            'login', 'logout', 'kill', 'death', 'assist', 'capture_flag',
            'complete_quest', 'purchase_item', 'level_up', 'join_match',
            'leave_match', 'earn_achievement', 'send_message', 'join_clan'
        ]
        
        # Игровые зоны/карты
        self.game_zones = ['Forest', 'Dungeon', 'Castle', 'Desert', 'Ice Cave', 'Volcano', 'City']
        
        # Типы оружия/классы
        self.weapon_types = ['Sword', 'Bow', 'Staff', 'Dagger', 'Axe', 'Wand', 'Shield']
        
    def connect_db(self):
        """Установить соединение с БД"""
        max_retries = 10
        for i in range(max_retries):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("Successfully connected to database")
                return conn
            except Exception as e:
                logger.warning(f"Connection attempt {i+1}/{max_retries} failed: {e}")
                time.sleep(3)
        raise Exception("Could not connect to database")
    
    def generate_game_event(self):
        """Сгенерировать игровое событие"""
        player = random.choice(self.players)
        event_type = random.choice(self.game_events)
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 3600))
        
        # Генерация данных в зависимости от типа события
        if event_type == 'kill':
            score_change = random.randint(10, 100)
            self.player_scores[player] += score_change
            details = {
                'victim': random.choice([p for p in self.players if p != player]),
                'weapon': random.choice(self.weapon_types),
                'zone': random.choice(self.game_zones)
            }
        elif event_type == 'death':
            score_change = random.randint(-50, -5)
            self.player_scores[player] += score_change
            details = {
                'killer': random.choice([p for p in self.players if p != player]),
                'zone': random.choice(self.game_zones)
            }
        elif event_type == 'level_up':
            self.player_levels[player] += 1
            score_change = random.randint(100, 500)
            self.player_scores[player] += score_change
            details = {
                'new_level': self.player_levels[player],
                'reward': random.choice(['gold', 'item', 'skill_point'])
            }
        elif event_type == 'complete_quest':
            score_change = random.randint(50, 300)
            self.player_scores[player] += score_change
            details = {
                'quest_name': f'Quest_{random.randint(1, 50)}',
                'difficulty': random.choice(['Easy', 'Medium', 'Hard']),
                'reward_gold': random.randint(10, 1000)
            }
        elif event_type == 'purchase_item':
            score_change = 0
            details = {
                'item': random.choice(['Health Potion', 'Mana Potion', 'Sword+1', 'Armor', 'Scroll']),
                'price': random.randint(5, 500),
                'currency': 'gold'
            }
        elif event_type in ['login', 'logout']:
            score_change = 0
            details = {
                'session_duration': random.randint(60, 7200) if event_type == 'logout' else 0,
                'platform': random.choice(['PC', 'Mobile', 'Console'])
            }
        else:
            score_change = random.randint(1, 50)
            self.player_scores[player] += score_change
            details = {
                'zone': random.choice(self.game_zones),
                'details': 'standard_event'
            }
        
        return {
            'event_id': str(uuid.uuid4()),
            'timestamp': timestamp,
            'player_id': player,
            'event_type': event_type,
            'score_change': score_change,
            'current_score': self.player_scores[player],
            'player_level': self.player_levels[player],
            'details': str(details),
            'game_zone': details.get('zone', random.choice(self.game_zones))
        }
    
    def save_to_db(self, conn, data):
        """Сохранить данные в БД"""
        try:
            cur = conn.cursor()
            query = """
            INSERT INTO game_events 
            (event_id, timestamp, player_id, event_type, score_change, 
             current_score, player_level, details, game_zone) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(query, (
                data['event_id'],
                data['timestamp'],
                data['player_id'],
                data['event_type'],
                data['score_change'],
                data['current_score'],
                data['player_level'],
                data['details'],
                data['game_zone']
            ))
            conn.commit()
            cur.close()
            logger.debug(f"Saved event: {data['player_id']} - {data['event_type']}")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            conn.rollback()
    
    def run(self):
        """Основной цикл генерации данных"""
        logger.info("Starting game data generator...")
        
        conn = self.connect_db()
        
        try:
            batch_size = 5  # Создаем по 5 событий за раз
            while True:
                for _ in range(batch_size):
                    data = self.generate_game_event()
                    self.save_to_db(conn, data)
                
                # Логируем каждые 25 событий
                if random.random() < 0.2:  # 20% chance
                    logger.info(f"Generated {batch_size} game events")
                
                time.sleep(random.uniform(1, 3))  # Случайная задержка 1-3 секунды
                
        except KeyboardInterrupt:
            logger.info("Stopping generator...")
        finally:
            conn.close()

if __name__ == "__main__":
    generator = GameDataGenerator()
    generator.run()
