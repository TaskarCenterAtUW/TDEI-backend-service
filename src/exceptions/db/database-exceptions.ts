export default class DatabaseException extends Error {
    constructor(message: string) {
        super(message);
    }
}
export class UniqueKeyDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}

export class ForeignKeyDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}

export class UndefinedColumnDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}

export class QuerySyntaxErrorDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}

export class UndefinedFunctionDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}

export class AmbiguousColumnDbException extends DatabaseException {
    constructor(message: string) {
        super(message);
    }
}