/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/26
 */

package internal

// refers to
// https://github.com/apache/tinkerpop/blob/master/gremlin-driver/src/main/java/org/apache/tinkerpop/gremlin/driver/Tokens.java
const (
	OPS_AUTHENTICATION = "authentication"
	OPS_BYTECODE       = "bytecode"
	OPS_EVAL           = "eval"
	OPS_INVALID        = "invalid"
	OPS_CLOSE          = "close"

	REQUEST_ID               = "requestId"
	ARGS_BATCH_SIZE          = "batchSize"
	ARGS_BINDINGS            = "bindings"
	ARGS_ALIASES             = "aliases"
	ARGS_FORCE               = "force"
	ARGS_GREMLIN             = "gremlin"
	ARGS_LANGUAGE            = "language"
	ARGS_SCRIPT_EVAL_TIMEOUT = "scriptEvaluationTimeout"
	ARGS_HOST                = "host"
	ARGS_SESSION             = "session"
	ARGS_MANAGE_TRANSACTION  = "manageTransaction"
	ARGS_SASL                = "sasl"
	ARGS_SASL_MECHANISM      = "saslMechanism"
	ARGS_SIDE_EFFECT         = "sideEffect"
	ARGS_AGGREGATE_TO        = "aggregateTo"
	ARGS_SIDE_EFFECT_KEY     = "sideEffectKey"

	VAL_AGGREGATE_TO_BULKSET = "bulkset"
	VAL_AGGREGATE_TO_LIST    = "list"
	VAL_AGGREGATE_TO_MAP     = "map"
	VAL_AGGREGATE_TO_NONE    = "none"
	VAL_AGGREGATE_TO_SET     = "set"

	VAL_TRAVERSAL_SOURCE_ALIAS = "g"

	STATUS_ATTRIBUTE_EXCEPTIONS  = "exceptions"
	STATUS_ATTRIBUTE_STACK_TRACE = "stackTrace"
	STATUS_ATTRIBUTE_WARNINGS    = "warnings"
)
