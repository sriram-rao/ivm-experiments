public class OrderDetail {
    int orderId;
    int productId;
    float unitPrice;
    int quantity;
    float discount;

    @Override
    public String toString() {
        return orderId + "," + quantity;
    }
}
