package br.com.denis.payments.event;

import java.time.Instant;
import java.util.UUID;

public record PagamentoSolicitado(
        UUID eventId,
        UUID pagamentoId,
        String contaId,
        long valorCentavos,
        String moeda,
        Instant ocorridoEm
) {
}
