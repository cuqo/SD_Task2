# SD_Task2
Algorisme que implementa una solució distribuïda d'exclusió mútua utilitzant IBM Cos. Les funcions slave actualitzen un fitxer comú anomenat "result.txt", afegint a aquest el número del seu identificador quan obtenguin permís per part de la funció master, l'encarregada de decidir a qui li toca escriure en cada moment.

El número d'slaves és totalment variable (fins a un màxim de 100), i pot ser fàcilment canviat modificant la variable global anomeada N_SLAVES
