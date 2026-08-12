# Política de compatibilidade

`is-wire-sea` segue versionamento semântico para a API pública Python e para o formato wire IS
existente. O nome da distribuição mudou, mas as aplicações continuam importando `is_wire`.

A versão 2.0 oferece suporte a Python 3.10 a 3.14, py-amqp 5.x, Protobuf 5 a 7 e RabbitMQ 4.3.
RabbitMQ 3.7.6 e 3.13.7 são alvos de transição e são testados apenas para viabilizar a migração
do broker.

A versão 2.0 remove o adaptador de exportação OpenCensus e `AsyncTransport`. O formato wire AMQP
e a propagação B3 continuam compatíveis com aplicações 1.x durante uma migração gradual.

As APIs `Subscription` e `Channel.consume` mantêm o comportamento legado de entrega no máximo
uma vez. `StreamSubscription` tem intencionalmente semântica de quadro mais recente com perdas.
Serviços RPC usam confirmações manuais e entrega de requisições pelo menos uma vez; handlers
devem ser idempotentes.

Corpos binários Protobuf e convenções existentes de propriedades AMQP permanecem estáveis na
série 2.x. Metadados aditivos são permitidos; remover ou reinterpretar um campo existente exige
uma versão major.
