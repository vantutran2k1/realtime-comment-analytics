CREATE TABLE IF NOT EXISTS comments_analyzed (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    author_name VARCHAR(255),
    author_channel_id VARCHAR(255),
    message TEXT,
    is_member BOOLEAN,
    prediction_label VARCHAR(50),
    confidence_score FLOAT,
    attack_type VARCHAR(50),
    target_entities VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);