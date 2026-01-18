-- Таблица игровых событий
CREATE TABLE IF NOT EXISTS game_events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    timestamp TIMESTAMP NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    score_change INTEGER NOT NULL,
    current_score INTEGER NOT NULL,
    player_level INTEGER NOT NULL,
    details TEXT,
    game_zone VARCHAR(50)
);

-- Индексы для быстрого поиска
CREATE INDEX idx_timestamp ON game_events(timestamp);
CREATE INDEX idx_player_id ON game_events(player_id);
CREATE INDEX idx_event_type ON game_events(event_type);
CREATE INDEX idx_game_zone ON game_events(game_zone);
CREATE INDEX idx_player_level ON game_events(player_level);

-- Таблица для агрегированной статистики (опционально, для демонстрации)
CREATE TABLE IF NOT EXISTS player_stats (
    player_id VARCHAR(50) PRIMARY KEY,
    total_score INTEGER DEFAULT 0,
    total_kills INTEGER DEFAULT 0,
    total_deaths INTEGER DEFAULT 0,
    total_play_time INTEGER DEFAULT 0,
    last_login TIMESTAMP,
    current_level INTEGER DEFAULT 1,
    favorite_zone VARCHAR(50)
);

-- Вьюха для быстрого доступа к топ-игрокам
CREATE OR REPLACE VIEW top_players AS
SELECT 
    player_id,
    MAX(current_score) as max_score,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'kill' THEN 1 ELSE 0 END) as total_kills,
    SUM(CASE WHEN event_type = 'death' THEN 1 ELSE 0 END) as total_deaths
FROM game_events
GROUP BY player_id
ORDER BY max_score DESC;

-- Функция для обновления статистики игрока
CREATE OR REPLACE FUNCTION update_player_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO player_stats (player_id, total_score, current_level)
    VALUES (NEW.player_id, NEW.current_score, NEW.player_level)
    ON CONFLICT (player_id) 
    DO UPDATE SET
        total_score = EXCLUDED.total_score,
        current_level = EXCLUDED.player_level,
        last_login = CASE 
            WHEN NEW.event_type = 'login' THEN NEW.timestamp 
            ELSE player_stats.last_login 
        END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления статистики
CREATE TRIGGER update_stats_trigger
AFTER INSERT ON game_events
FOR EACH ROW
EXECUTE FUNCTION update_player_stats();
