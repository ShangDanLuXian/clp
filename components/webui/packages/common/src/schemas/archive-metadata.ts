import {Type} from "@sinclair/typebox";

import {StringSchema} from "./common.js";


/**
 * Schema for SQL query request.
 */
const SqlSchema = Type.Object({
    queryParams: Type.Optional(Type.Array(Type.Unknown())),
    queryString: StringSchema,
});


export {SqlSchema};
