# **Experiment 1** - 1 Client

Server bindet an zwei Adressen, Client sendet Hello world im Loop und sendet *STOP* an Server.
Server bricht aus dem listening loop aufgrund von STOP Signal

# **Experiment 1** - 2 Clients
client1.py sendet Nachricht an Server und blockiert bis Server echo zurücksendet.
Wenn man die Reihenfolge client.py -> client1.py -> server.py ausführt, blockiert client1.py endlos,
da server.py aufgrund von client.py stoppt.