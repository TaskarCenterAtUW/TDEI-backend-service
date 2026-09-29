Put the union `.sql` you want to test here, e.g. `union_v5_clean.sql`.
`*.sql` in this folder is git-ignored: the union is versioned with the union
code, so copy in whichever version you are testing.

The local Docker database deploys every `.sql` here the first time it starts.
After editing, redeploy with:

    ./local_db.sh deploy union/<file>.sql

See docs/setup.md.
