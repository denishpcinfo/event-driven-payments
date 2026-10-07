package br.com.denis.payments.api;

import br.com.denis.payments.application.SolicitarPagamentoService;
import jakarta.validation.Valid;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pagamentos")
public class PagamentoController {

    private final SolicitarPagamentoService service;

    public PagamentoController(SolicitarPagamentoService service) {
        this.service = service;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<PagamentoAceitoResponse>> solicitar(
            @Valid @RequestBody SolicitarPagamentoRequest request) {
        return service.solicitar(request.contaId(), request.valorCentavos(), request.moeda())
                .thenApply(evento -> ResponseEntity.accepted().body(
                        new PagamentoAceitoResponse(
                                evento.pagamentoId(), evento.eventId(), "SOLICITADO")));
    }
}
