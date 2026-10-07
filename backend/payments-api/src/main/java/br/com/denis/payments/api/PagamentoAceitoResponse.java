package br.com.denis.payments.api;

import java.util.UUID;

public record PagamentoAceitoResponse(
        UUID pagamentoId,
        UUID eventId,
        String status
) {
}
