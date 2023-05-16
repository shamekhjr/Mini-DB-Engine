grammar SQLParser;

file
    : queries + EOF
    ;

queries
    : statement+
    ;

statement
    : create
    | select
    | update
    | delete
    | insert
    ;

create
    : CREATE TABLE tableName (IDENTIFIER DATATYPE (COMMA IDENTIFIER DATATYPE)*)
    ;

insert
    : INSERT INTO tableName (columnNames) VALUES (values)
    ;

delete
    : DELETE FROM tableName (WHERE booleanExpression)*
    ;

update
    : UPDATE tableName SET (IDENTIFIER EQUALS constant  (COMMA IDENTIFIER EQUALS constant)*) (WHERE booleanExpression)*
    ;

select
    : SELECT  columnList  FROM tableName (WHERE booleanExpression)*
    ;


values
    : constant (COMMA constant)*
    ;

columnList
    : columnNames | '*';

columnNames
    : IDENTIFIER  (COMMA IDENTIFIER)*
    ;

tableName
    : IDENTIFIER
    ;

booleanExpression
    : IDENTIFIER compare constant
    | left=booleanExpression operator=AND right=booleanExpression
    | left=booleanExpression operator=OR right=booleanExpression
    | left=booleanExpression operator=XOR right=booleanExpression
    ;

compare:
    EQUALS | GT | GE| LT| LE | NE;

constant
    : NULL
    | INTEGER_VALUE
    | DECIMAL_DIGITS
    | QUOTED_STRING+
    ;

CREATE : 'CREATE' | 'create';
SELECT: 'SELECT' | 'select';
FROM: 'FROM' | 'from';
INSERT : 'INSERT' | 'insert';
INTO : 'INTO' | 'into';
UPDATE : 'UPDATE' | 'update';
SET : 'SET' | 'set';
WHERE : 'WHERE' | 'where';
DELETE : 'DELETE' | 'delete';
NULL : 'NULL' | 'null';
STAR : '*';


COMMA: ',';
EQUALS: '=';
GT: '>';
GE: '>=';
LT: '<';
LE: '<=';
NE: '!=';

AND: 'AND' | 'and';
OR: 'OR' | 'or';
XOR: 'XOR' | 'xor';

QUOTED_STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS
    ;

IDENTIFIER
    : (LETTER | DIGIT)+
    ;

DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DIGIT
    : [0-9]
    ;

LETTER
    : [a-zA-Z]
    ;

DATATYPE
    : INT
    | DOUBLE
    | DATE
    | VARCHAR
    ;

INT : 'INTEGER' | 'INT' | 'Integer' | 'INT' | 'integer' | 'int';
DOUBLE : 'DOUBLE' | 'Double' | 'double';
DATE : 'DATE' | 'Date' | 'date';
VARCHAR : 'VARCHAR' | 'Varchar' | 'varchar';

WhiteSpace  : [ \t\r\n]+ -> skip;
