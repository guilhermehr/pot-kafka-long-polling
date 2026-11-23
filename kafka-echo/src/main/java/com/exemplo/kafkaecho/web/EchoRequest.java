package com.exemplo.kafkaecho.web;

import jakarta.validation.constraints.NotBlank;

public record EchoRequest(@NotBlank String message) {
}
