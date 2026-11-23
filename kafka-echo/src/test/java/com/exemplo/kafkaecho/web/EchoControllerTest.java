package com.exemplo.kafkaecho.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

import com.exemplo.kafkaecho.TestProps;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import com.exemplo.kafkaecho.producer.EchoProducerService;
import jakarta.servlet.AsyncEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockAsyncContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class EchoControllerTest {

    @Mock
    private EchoProducerService producer;

    private final PendingRequestRegistry registry = new PendingRequestRegistry();
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(new EchoController(registry, producer, TestProps.props()))
                .build();
    }

    private MvcResult postMessage(String json) throws Exception {
        return mockMvc.perform(post("/api/echo").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(request().asyncStarted())
                .andReturn();
    }

    @Test
    void returnsPayloadWhenPendingRequestIsCompleted() throws Exception {
        when(producer.publish(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        MvcResult result = postMessage("{\"message\":\"hello\"}");

        ArgumentCaptor<String> id = ArgumentCaptor.forClass(String.class);
        verify(producer).publish(id.capture(), eq("hello"));
        assertThat(registry.size()).isEqualTo(1);

        assertThat(registry.complete(id.getValue(), "hello-back")).isTrue();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isOk())
                .andExpect(content().string("{\"message\":\"hello-back\"}"));
        assertThat(registry.size()).isZero();
    }

    @Test
    void generatesDistinctCorrelationIdPerRequest() throws Exception {
        when(producer.publish(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        postMessage("{\"message\":\"a\"}");
        postMessage("{\"message\":\"b\"}");

        ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
        verify(producer, org.mockito.Mockito.times(2)).publish(ids.capture(), anyString());
        assertThat(ids.getAllValues()).doesNotHaveDuplicates();
        assertThat(registry.size()).isEqualTo(2);
    }

    @Test
    void returnsGatewayTimeoutAndCleansRegistryWhenNoReplyArrives() throws Exception {
        when(producer.publish(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        MvcResult result = postMessage("{\"message\":\"hello\"}");
        assertThat(registry.size()).isEqualTo(1);

        MockAsyncContext asyncContext = (MockAsyncContext) result.getRequest().getAsyncContext();
        asyncContext.getListeners().forEach(listener -> {
            try {
                listener.onTimeout(new AsyncEvent(asyncContext));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        mockMvc.perform(asyncDispatch(result)).andExpect(status().isGatewayTimeout());
        assertThat(registry.size()).isZero();
    }

    @Test
    void returnsBadGatewayWhenPublishFails() throws Exception {
        when(producer.publish(anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        MvcResult result = postMessage("{\"message\":\"hello\"}");

        mockMvc.perform(asyncDispatch(result)).andExpect(status().isBadGateway());
        assertThat(registry.size()).isZero();
    }

    @Test
    void rejectsBlankMessageWithBadRequest() throws Exception {
        mockMvc.perform(post("/api/echo").contentType(MediaType.APPLICATION_JSON).content("{\"message\":\"  \"}"))
                .andExpect(status().isBadRequest());

        verify(producer, never()).publish(anyString(), anyString());
        assertThat(registry.size()).isZero();
    }
}
