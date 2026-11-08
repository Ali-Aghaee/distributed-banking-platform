INSERT INTO accounts(id, balance) SELECT 'A' || lpad(g::text, 4, '0'), 1000.00 FROM generate_series(1, 1000) g;
INSERT INTO chunks SELECT c, c*100+1, c*100+100, 'ACTIVE', 1 FROM generate_series(0, 9) c;
TRUNCATE changelog;
