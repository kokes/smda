# pg protocol implementation

use wireshark, port 8824?

https://www.postgresql.org/docs/13/protocol-flow.html
https://www.postgresql.org/docs/13/protocol-message-formats.html
string et al https://www.postgresql.org/docs/12/protocol-message-types.html

images here: https://ankushchadda.in/posts/postgres-understanding-the-wire-protocol/
nice expl https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf

the current implementation can be connected to from psql (no validation of anything), but no queries can be
made: we accept query strings, but have no way of responding

type OIDs in RowDescription are probably in pg_type table in pg


refactor:
- all messages start with an ASCII identifier, then length, then payload
- the startup message is the only exception (no ASCII identifier)
- take these two things into account
