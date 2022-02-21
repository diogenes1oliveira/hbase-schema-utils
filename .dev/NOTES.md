- HBase UI with React?
- JOLT, Groovy, json path, etc to yield a generic Schema?
- Kerberized HBase docker in this very repo?
- Supervisord to handle the start/stopping of the Docker container;
- Wrap the connections in WebSocket? This would help with DNS and authorization, etc
- Devise a scheme to fetch the Kerberos context and transmitting it via SPNEGO/LDAP/SSH etc
- Queries HBase via Spark => I can use these schema mappers as well

Realistas:

- Verificar pacote org.apache.hadoop.fs.shell.find.Result
- Checar interrupted;
- Abstract provavelmente devia ser interface;
- Consertar o builder, criar a estrutura para o write pojo
- Pacote separado para as interfaces;
- FAzer uns schemas que sempre lançam unsupportedoperation para não ter que usar nulo em testes;
- Verificar um timeout no contextlock para não dar erro eterno em caso de uso errado do contexto;
