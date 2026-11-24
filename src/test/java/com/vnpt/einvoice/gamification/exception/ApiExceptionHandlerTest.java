package com.vnpt.einvoice.gamification.exception;

import com.vnpt.einvoice.gamification.invoice.InvoiceController;
import com.vnpt.einvoice.gamification.invoice.InvoiceEventPublisher;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardController;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ApiExceptionHandlerTest {

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                        new InvoiceController(mock(InvoiceEventPublisher.class)),
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
    void unsupportedMediaTypeReturnsUnsupportedMediaType() throws Exception {
        mvc.perform(post("/api/invoices/simulate")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("{}"))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(jsonPath("$.status").value(415));
    }
}
