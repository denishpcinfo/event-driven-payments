package br.com.denis.payments.application;

import br.com.denis.payments.event.PagamentoSolicitado;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class SolicitarPagamentoService {

    private static final Logger log = LoggerFactory.getLogger(SolicitarPagamentoService.class);

    private final KafkaTemplate<String, PagamentoSolicitado> kafkaTemplate;
    private final String topico;

    public SolicitarPagamentoService(
            KafkaTemplate<String, PagamentoSolicitado> kafkaTemplate,
            @Value("${app.kafka.topics.pagamentos-solicitados}") String topico) {
        this.kafkaTemplate = kafkaTemplate;
        this.topico = topico;
    }

    public CompletableFuture<PagamentoSolicitado> solicitar(
            String contaId, long valorCentavos, String moeda) {
        var evento = new PagamentoSolicitado(
                UUID.randomUUID(), UUID.randomUUID(), contaId,
                valorCentavos, moeda, Instant.now());

        CompletableFuture<SendResult<String, PagamentoSolicitado>> envio;
        try {
            // A chave e enviada separadamente do JSON: um campo no payload nao vira chave.
            envio = kafkaTemplate.send(topico, contaId, evento);
        } catch (KafkaException | org.springframework.kafka.KafkaException ex) {
            // Algumas falhas acontecem antes de send() devolver o future.
            return CompletableFuture.failedFuture(falha(evento, ex));
        }

        return envio.handle((resultado, erro) -> {
            if (erro != null) {
                throw falha(evento, erro);
            }

            var metadata = resultado.getRecordMetadata();
            log.info("evento_publicado eventId={} pagamentoId={} topico={} particao={} offset={}",
                    evento.eventId(), evento.pagamentoId(), metadata.topic(),
                    metadata.partition(), metadata.offset());

            // Somente disponibiliza o resultado ao controller depois da confirmacao do Kafka.
            return evento;
        });
    }

    private PublicacaoPagamentoException falha(PagamentoSolicitado evento, Throwable causa) {
        log.error("falha_publicacao eventId={} pagamentoId={}",
                evento.eventId(), evento.pagamentoId(), causa);
        return new PublicacaoPagamentoException(evento.pagamentoId(), evento.eventId(), causa);
    }
}
