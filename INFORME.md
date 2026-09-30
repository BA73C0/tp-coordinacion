Para la coordianacion entre los nodos de Sum, Agg y Join traté de ceñirme lo máximo posible a la consigna del enunciado: 
Sum: "[...] envía los pares (fruta, cantidad) totales a los Aggregators"
Agg: "[...] se calcula un top parcial y se envía esa información al Joiner"
Join: "[...] envía el top final hacia el gateway [...]"

Con eso en mente la comunicación varía entre los nodos. Entre los Sum y los Agg hay un exchange, donde utilizo una función de hashing para saber a que Agg debe ir cada combinacion de "Cliente-Fruta". Esto permite que cada Agg pueda calcular un top parcial con las frutas que tiene del cliente. Luego las envía al Join a través de una queue y este calcula el top global que le devuelve al gateway.

Para coordinar la finalizacion del stream de datos (EOF) planteé una solución diferente en cada nodo. 
Sum: 
    Al recibir un `EOF` desde el gateway, le avisa al resto de nodos Sum que llegó el final del cliente X y luego espera los Ack de los vecinos. Estos acks vienen con la cantidad de mensajes que recibió cada nodo de ese cliente X. Cuando el nodo que recibió el primer EOF verifica que estén todos los mensajes de ese cliente y avisa a los nodos Sum que envíen sus sumas a los nodos Agg.
    Cabe mencionar que el protocolo no soporta pérdidas de paquetes

Agg:
    Acá fue más simple, cuando un Agg recibe `SUM_AMOUNT` de EOF, calcula el top parcial y lo envía al Join

Join:
    Igual que en Agg, cuando recibe `AGGREGATION_AMOUNT` de EOF, toma el top global y lo envia al gateway

Por último, para poder aprovechar al maximo la cantidad de nodos, el protocolo que implementé evita reunir flujos de datos: 
- Cada sum puede recibir datos de cualquier combinacion "Cliente-Fruta", lo que permite aumentar su cantidad para mejorar el throughput de datos
- Con la función de hash, permito que cada Agg solo reciba un subconjunto de "Clientes" y "Frutas"; permitiendo procesar muchos clientes y/o muchas frutas