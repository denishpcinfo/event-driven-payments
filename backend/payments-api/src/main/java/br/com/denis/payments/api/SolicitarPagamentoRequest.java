package br.com.denis.payments.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record SolicitarPagamentoRequest(
        @NotBlank @Size(max = 80) String contaId,
        @NotNull @Positive Long valorCentavos,
        @NotBlank @Pattern(regexp = "BRL", message = "deve ser BRL neste laboratorio") String moeda
) {
}
