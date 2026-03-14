{%  macro clone_db(new_db, src_db)  %}
    {%  set sqlquery    %}
        create or replace database {{ new_db }} clone {{ src_db }}
    {%  endset  %}
    {% do run_query(sqlquery) %}

{%  endmacro    %}