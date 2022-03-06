- HBase UI with React?
- JOLT, Groovy, json path, etc to yield a generic Schema?
- Kerberized HBase docker in this very repo?
- Supervisord to handle the start/stopping of the Docker container;
- Wrap the connections in WebSocket? This would help with DNS and authorization, etc
- Devise a scheme to fetch the Kerberos context and transmitting it via SPNEGO/LDAP/SSH etc
- Queries HBase via Spark => I can use these schema mappers as well
- Autopaginação no schema conector;
- GraphQL? UI?

Realistas:

- Consertar o report do JaCoCo;
- Decorator time???
- Verificar pacote org.apache.hadoop.fs.shell.find.Result
- Mergear os .exec do JaCoCo;
- JavaDoc warnings;
- Separate conversions to a different module?

Feito:

- Checar interrupted;
- Consertar Conversions, too cumbersome all of those stringGetter() longGetter() etc;
- Pacote separado para as interfaces e exceções;
- Verificar um timeout no contextlock para não dar erro eterno em caso de uso errado do contexto;
- Verificar porque está sendo rodado o TestContainers no maven => era só o .env não carregando na linha de comando;

Desistido:

- Abstract provavelmente devia ser interface => melhor não, deixa um SchemaProvider cuidar disso
- Tiemstamp shouldn't be nullable, really; => it can, best to deal with optional fields
- Fazer uns schemas que sempre lançam unsupportedoperation para não ter que usar nulo em testes não mais necessário => não precisa no novo
  formato;
