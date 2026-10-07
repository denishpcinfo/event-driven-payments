package br.com.denis.payments.application;

import java.util.UUID;

public class PublicacaoPagamentoException extends RuntimeException {

    private final UUID pagamentoId;
    private final UUID eventId;

    public PublicacaoPagamentoException(UUID pagamentoId, UUID eventId, Throwable cause) {
        super("Nao foi possivel confirmar a publicacao do pagamento", cause);
        this.pagamentoId = pagamentoId;
        this.eventId = eventId;
    }

    public UUID getPagamentoId() {
        return pagamentoId;
    }

    public UUID getEventId() {
        return eventId;
    }
}
