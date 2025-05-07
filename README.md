Todos:
- Implement scanning for conditional blocks (IF/ELIF/ELSE).
- Create a global state object for an actor (required for python, and auto track things across connections)
- Bring python in.
- WebSocket support for JS testing
- Pretty errors using `codespan-reporting`.
- Write tests
- When `!: ALLOW CONCURRENT`/`!: ALLOW RESTART`, `S: <EXIT>` should only kill the current connection
- Fix connection id auto response not incrementing (maybe `!: ALLOW CONCURRENT` is required to reproduce?)

notes:
- master vim.
