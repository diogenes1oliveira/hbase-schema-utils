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
- Pacote separado para as interfaces e exceções;
- Verificar um timeout no contextlock para não dar erro eterno em caso de uso errado do contexto;

- Abstract provavelmente devia ser interface => melhor não, deixa um SchemaProvider cuidar disso
- Tiemstamp shouldn't be nullable, really; => it can, best to deal with optional fields
- Fazer uns schemas que sempre lançam unsupportedoperation para não ter que usar nulo em testes não mais necessário => não precisa no 
  novo formato;
