package com.exemplo.kafkaecho.producer;

/** A publicação esperou na fila por mais que o timeout da requisição e foi descartada sem enviar. */
public class PublishExpiredException extends RuntimeException {

    public PublishExpiredException(String message) {
        super(message);
    }
}
