package com.vnpt.einvoice.gamification.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public record InvoiceBatchRequest(
        @NotEmpty @Size(max = 1000) List<@NotNull @Valid InvoiceSimulationRequest> invoices
) {
}
