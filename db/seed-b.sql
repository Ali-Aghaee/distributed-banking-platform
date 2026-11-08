INSERT INTO accounts(id, balance) SELECT 'A' || lpad(g::text, 4, '0'), 1000.00 FROM generate_series(1001, 2000) g;
INSERT INTO chunks SELECT c, c*100+1, c*100+100, 'ACTIVE', 1 FROM generate_series(10, 19) c;
TRUNCATE changelog;
