-- Payments
CREATE TABLE raw.olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type TEXT,
    payment_installments INT,
    payment_value NUMERIC
);