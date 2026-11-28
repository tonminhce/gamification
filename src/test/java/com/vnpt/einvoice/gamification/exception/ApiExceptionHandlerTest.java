package com.vnpt.einvoice.gamification.exception;

import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.invoice.InvoiceController;
import com.vnpt.einvoice.gamification.invoice.InvoiceEventPublisher;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardController;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ApiExceptionHandlerTest {

    private MockMvc mvc;
    private InvoiceEventPublisher publisher;
    private LeaderboardService leaderboardService;

    @BeforeEach
    void setUp() {
        publisher = mock(InvoiceEventPublisher.class);
        leaderboardService = mock(LeaderboardService.class);
        mvc = MockMvcBuilders.standaloneSetup(
                        new InvoiceController(publisher),
                        new LeaderboardController(leaderboardService)
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

    @Test
    void overloadReturnsServiceUnavailableWithRetryAfterHeader() throws Exception {
        when(publisher.publish(any())).thenThrow(new OverloadException("saturated"));

        mvc.perform(post("/api/invoices/simulate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"invoiceId\":\"INV-1\",\"companyId\":\"COMPANY-A\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    void kafkaPublishFailureReturnsServiceUnavailable() throws Exception {
        when(publisher.publish(any())).thenThrow(new EventPublishException("Kafka rejected the invoice event", new RuntimeException("down")));

        mvc.perform(post("/api/invoices/simulate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"invoiceId\":\"INV-1\",\"companyId\":\"COMPANY-A\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.message").value("Kafka rejected the invoice event"));
    }

    @Test
    void redisFailureReturnsServiceUnavailable() throws Exception {
        when(leaderboardService.top(10)).thenThrow(new DataAccessResourceFailureException("Redis down"));

        mvc.perform(get("/api/leaderboard/top").param("limit", "10"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.message").value("Redis is unavailable"));
    }
}
