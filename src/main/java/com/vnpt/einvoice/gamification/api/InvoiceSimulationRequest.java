package com.vnpt.einvoice.gamification.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record InvoiceSimulationRequest(
        @NotBlank @Size(max = 64) String invoiceId,
        @NotBlank @Size(max = 64) String companyId
) {
}
