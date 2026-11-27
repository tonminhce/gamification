package com.vnpt.einvoice.gamification.exception;

import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.invoice.InvoiceController;
import com.vnpt.einvoice.gamification.invoice.InvoiceEventPublisher;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardController;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ApiExceptionHandlerTest {

    private MockMvc mvc;
    private InvoiceEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = mock(InvoiceEventPublisher.class);
        mvc = MockMvcBuilders.standaloneSetup(
                        new InvoiceController(publisher),
                        new LeaderboardController(mock(LeaderboardService.class))
                )
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void malformedJsonReturnsBadRequest() throws Exception {
        mvc.perform(post("/api/invoices/simulate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{invalid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void nonNumericLimitReturnsBadRequest() throws Exception {
        mvc.perform(get("/api/leaderboard/top").param("limit", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void emptyBatchReturnsBadRequest() throws Exception {
        mvc.perform(post("/api/invoices/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"invoices\":[]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void batchWithNullInvoiceReturnsBadRequest() throws Exception {
        mvc.perform(post("/api/invoices/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"invoices\":[null]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void validBatchReturnsAccepted() throws Exception {
        when(publisher.publishBatch(anyList()))
                .thenReturn(new InvoiceBatchAcceptedResponse(2, "PUBLISHING"));

        mvc.perform(post("/api/invoices/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"invoices\":[{\"invoiceId\":\"INV-1\",\"companyId\":\"COMPANY-A\"},{\"invoiceId\":\"INV-2\",\"companyId\":\"COMPANY-B\"}]}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.acceptedCount").value(2))
                .andExpect(jsonPath("$.status").value("PUBLISHING"));
    }

    @Test
    void unsupportedMediaTypeReturnsUnsupportedMediaType() throws Exception {
        mvc.perform(post("/api/invoices/simulate")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("{}"))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(jsonPath("$.status").value(415));
    }
}
