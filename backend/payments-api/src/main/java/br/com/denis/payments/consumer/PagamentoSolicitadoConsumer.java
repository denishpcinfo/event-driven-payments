package br.com.denis.payments.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PagamentoSolicitadoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PagamentoSolicitadoConsumer.class);

    @KafkaListener(topics = "${app.kafka.topics.pagamentos-solicitados}")
    public void consumir(ConsumerRecord<String, String> registro) {
        // Nesta etapa, receber significa ler e registrar o evento no log.
        // O processamento do pagamento sera implementado em uma etapa posterior.
        log.info(
                "PAGAMENTO_RECEBIDO topico={} particao={} offset={} chave={} evento={}",
                registro.topic(),
                registro.partition(),
                registro.offset(),
                registro.key(),
                registro.value());
    }
}
