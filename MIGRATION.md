# Migrando para is-wire-sea 2.0

## Pacote Python

Substitua `is-wire` por `is-wire-sea` nos manifestos de dependências. Não altere os imports da
aplicação: o pacote continua sendo importado como `is_wire`. Atualize o runtime para Python 3.10
ou superior.

A compatibilidade com OpenCensus e `AsyncTransport` foi removida na 2.0. As aplicações devem usar
OpenTelemetry por meio de `is-wire-sea[tracing]` ou a integração Zipkin agrupada por meio de
`is-wire-sea[zipkin]`.

## Filas

Assinaturas anônimas agora declaram filas transitórias exclusivas. Assinaturas nomeadas e
serviços RPC declaram filas duráveis compartilhadas, com expiração após cinco minutos sem uso.
Se ainda existir uma fila nomeada com as propriedades não duráveis antigas, pare seus
consumidores e exclua essa fila antes de iniciar a 2.0; o RabbitMQ rejeita a redeclaração com
propriedades diferentes.

Mantenha `Subscription` para configurações/eventos em que todo assinante precisa de uma cópia.
Altere consumidores de imagens em tempo real para `StreamSubscription` e atribua um grupo
estável por etapa de processamento. Todas as réplicas de uma etapa devem usar exatamente o
mesmo grupo.

## Broker

Valide primeiro a 2.0 contra a implantação existente do RabbitMQ. Um diretório de dados do
RabbitMQ 3.7.6 não pode ser atualizado diretamente para a 4.3. Crie um cluster 4.3 novo e
fixado, reproduza usuários/vhosts, permissões, políticas e exchanges, e migre as aplicações
gradualmente. Não habilite `transient_nonexcl_queues` como configuração permanente.

## Verificações operacionais

- Use canais/conexões separados para tráfego de imagens em alto volume e RPC.
- Confirme que publicadores de imagens enviam bytes JPEG, WebP ou PNG, e não arrays brutos ou
  base64.
- Verifique se cada etapa replicada compartilha um grupo de streaming.
- Monitore bytes publicados/recebidos, idade dos quadros, erros de callback, status RPC e
  reconexões.
- Trate handlers RPC como idempotentes, pois requisições não confirmadas podem ser reentregues.
