package org.example.orderservice.models.services.impl;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.example.orderservice.models.constants.OrderStatus;
import org.example.orderservice.models.dto.requests.CreateOrderRequest;
import org.example.orderservice.models.dto.responses.OrderDetailResponse;
import org.example.orderservice.models.dto.responses.OrderResponse;
import org.example.orderservice.models.entities.Order;
import org.example.orderservice.models.entities.OrderDetail;
import org.example.orderservice.models.repositories.OrderDetailRepository;
import org.example.orderservice.models.repositories.OrderRepository;
import org.example.orderservice.models.services.OrderService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    private final OrderDetailRepository orderDetailRepository;
    private final ProductGatewayService productGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    @Override
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        List<OrderDetail> details = new ArrayList<>();
        List<String> productNames = new ArrayList<>();
        BigDecimal total = BigDecimal.ZERO;
        for (var item : request.items()) {
            var product = productGatewayService.getProductById(item.productId());
            BigDecimal subtotal = BigDecimal.valueOf(product.price()).multiply(BigDecimal.valueOf(item.quantity()));
            total = total.add(subtotal);
            details.add(OrderDetail.builder().productId(product.id()).quantity(item.quantity()).unitPrice(product.price()).build());
            productNames.add(product.name());
        }
        Order order = orderRepository.save(Order.builder().customerName(request.customerName().trim()).total(total.doubleValue()).status(OrderStatus.PENDING).build());
        details.forEach(detail -> detail.setOrder(order));
        List<OrderDetail> savedDetails = orderDetailRepository.saveAll(details);
        List<OrderDetailResponse> items = new ArrayList<>();
        for (int i = 0; i < savedDetails.size(); i++) {
            OrderDetail detail = savedDetails.get(i);
            double subtotal = BigDecimal.valueOf(detail.getUnitPrice()).multiply(BigDecimal.valueOf(detail.getQuantity())).doubleValue();
            items.add(new OrderDetailResponse(detail.getId(), detail.getProductId(), productNames.get(i), detail.getQuantity(), detail.getUnitPrice(), subtotal));
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send("order-created", request.customerEmail()).join();
            }
        });
        return new OrderResponse(order.getId(), order.getCustomerName(), order.getTotal(), order.getStatus(), items);
    }
}
