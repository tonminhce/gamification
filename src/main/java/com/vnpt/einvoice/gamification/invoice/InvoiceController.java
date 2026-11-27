package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchRequest;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/invoices")
public class InvoiceController {

    private final InvoiceEventPublisher publisher;

    public InvoiceController(InvoiceEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/simulate")
    ResponseEntity<InvoiceAcceptedResponse> simulate(@Valid @RequestBody InvoiceSimulationRequest request) {
        return ResponseEntity.accepted().body(publisher.publish(request));
    }

    @PostMapping("/batch")
    ResponseEntity<InvoiceBatchAcceptedResponse> batch(@Valid @RequestBody InvoiceBatchRequest request) {
        return ResponseEntity.accepted().body(publisher.publishBatch(request.invoices()));
    }
}
