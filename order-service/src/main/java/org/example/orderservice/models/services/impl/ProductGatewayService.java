package org.example.orderservice.models.services.impl;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import org.example.orderservice.clients.ProductClient;
import org.example.orderservice.exceptions.ProductNotFoundException;
import org.example.orderservice.exceptions.ProductServiceException;
import org.example.orderservice.models.dto.responses.ProductResponse;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Service;
@Service
@RequiredArgsConstructor
public class ProductGatewayService {
    private final ProductClient productClient;
    private final CircuitBreakerFactory<?, ?> circuitBreakerFactory;
    public ProductResponse getProductById(Long productId) {
        return circuitBreakerFactory.create("productService").run(() -> {
            ProductResponse product = productClient.getProductById(productId);
            if (product == null || !productId.equals(product.id()) || product.name() == null || product.name().isBlank() || product.price() == null || !Double.isFinite(product.price()) || product.price() < 0) {
                throw new ProductServiceException("Invalid product data for id: " + productId);
            }
            return product;
        }, failure -> getProductByIdFallback(productId, failure));
    }
    public ProductResponse getProductByIdFallback(Long productId, Throwable failure) {
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof ProductNotFoundException notFound) {
                throw notFound;
            }
            if (cause instanceof FeignException feignException && feignException.status() == 404) {
                throw new ProductNotFoundException(productId);
            }
            if (cause instanceof ProductServiceException serviceException) {
                throw serviceException;
            }
            cause = cause.getCause();
        }
        throw new ProductServiceException("Product service is unavailable for id: " + productId, failure);
    }
}
