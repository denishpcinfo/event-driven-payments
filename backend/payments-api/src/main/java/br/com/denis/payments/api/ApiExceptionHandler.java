package br.com.denis.payments.api;

import br.com.denis.payments.application.PublicacaoPagamentoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(PublicacaoPagamentoException.class)
    public ProblemDetail publicacao(PublicacaoPagamentoException ex) {
        var problema = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Nao foi possivel confirmar a publicacao. O resultado do envio pode ser indeterminado.");
        problema.setTitle("Publicacao nao confirmada");
        problema.setProperty("pagamentoId", ex.getPagamentoId());
        problema.setProperty("eventId", ex.getEventId());
        return problema;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail validacao(MethodArgumentNotValidException ex) {
        var problema = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Revise os campos da solicitacao.");
        problema.setTitle("Solicitacao invalida");
        problema.setProperty("erros", ex.getBindingResult().getFieldErrors().stream()
                .map(erro -> new ErroCampo(erro.getField(), erro.getDefaultMessage()))
                .toList());
        return problema;
    }

    public record ErroCampo(String campo, String mensagem) {
    }
}
