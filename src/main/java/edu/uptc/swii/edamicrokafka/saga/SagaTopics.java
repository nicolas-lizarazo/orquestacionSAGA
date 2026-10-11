package edu.uptc.swii.edamicrokafka.saga;

public final class SagaTopics {
    private SagaTopics() {}

    public static final String VALIDATE_CUSTOMER_CMD = "validate_customer_cmd";
    public static final String VALIDATE_CUSTOMER_REPLY = "validate_customer_reply";
    public static final String PAYMENT_CMD = "payment_cmd";
    public static final String PAYMENT_REPLY = "payment_reply";
    public static final String PAYMENT_REFUND_CMD = "payment_refund_cmd";
    public static final String ORDER_EVENTS = "order_events";

    public static final String OP_VALIDATE_CUSTOMER = "VALIDATE_CUSTOMER";
    public static final String OP_VALIDATE_CUSTOMER_REPLY = "VALIDATE_CUSTOMER_REPLY";
    public static final String OP_PAY = "PAY";
    public static final String OP_PAY_REPLY = "PAY_REPLY";
    public static final String OP_REFUND = "REFUND";
    public static final String OP_STATUS_UPDATE = "STATUS_UPDATE";

    public static final String OK = "OK";
    public static final String FAIL = "FAIL";
}