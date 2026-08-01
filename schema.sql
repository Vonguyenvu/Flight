CREATE TABLE flight_states (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,                  -- thời điểm snapshot được ghi nhận
    icao24 VARCHAR(10) NOT NULL,                    -- [0] Mã máy bay
    callsign VARCHAR(10),                           -- [1] Hô hiệu (đã trim khoảng trắng)
    longitude DOUBLE PRECISION,                      -- [5] Kinh độ
    latitude DOUBLE PRECISION,                       -- [6] Vĩ độ
    altitude REAL,                             -- [7] Độ cao (m)
    on_ground BOOLEAN,                              -- [8] Trên mặt đất (true/false)
    velocity REAL                                   -- [9] Tốc độ (m/s)
);

-- Tạo Index tăng tốc query theo thời gian và mã máy bay
CREATE INDEX idx_flight_icao24 ON flight_states(icao24);
CREATE INDEX idx_flight_event_time ON flight_states(event_time);