import { readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { brandString } from "@deepseek-ai/dsh-brand";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import { defineTool } from "@deepseek-ai/dsh-tools";
import { HarnessSessionHost, get_shared_memory, probe, register_graph_tools, register_timeline_tools } from "@deepseek-ai/dsh-webhook-weixin";
//#region ../../../vendor/cosmokit/src/misc.ts
/** Return true when a value is `null` or `undefined`. */
function isNullable(value) {
	return value === null || value === void 0;
}
/** Return true for non-array object values. */
function isPlainObject(data) {
	return data && typeof data === "object" && !Array.isArray(data);
}
/** Filter object entries and return a new object. */
function filterKeys(object, filter) {
	return Object.fromEntries(Object.entries(object).filter(([key, value]) => filter(key, value)));
}
/** Map object values while preserving the original key set. */
function mapValues(object, transform) {
	return Object.fromEntries(Object.entries(object).map(([key, value]) => [key, transform(value, key)]));
}
/** Pick selected keys from an object, optionally including `undefined` values. */
function pick(source, keys, forced) {
	if (!keys) return { ...source };
	const result = {};
	for (const key of keys) if (forced || source[key] !== void 0) result[key] = source[key];
	return result;
}
//#endregion
//#region ../../../vendor/cosmokit/src/types.ts
/** Test values using `instanceof` with a `toStringTag` fallback. */
function is(type, value) {
	if (arguments.length === 1) return (value) => is(type, value);
	return type in globalThis && value instanceof globalThis[type] || Object.prototype.toString.call(value).slice(8, -1) === type;
}
function isArrayBufferLike(value) {
	return is("ArrayBuffer", value) || is("SharedArrayBuffer", value);
}
function isArrayBufferSource(value) {
	return isArrayBufferLike(value) || ArrayBuffer.isView(value);
}
let Binary;
(function(_Binary) {
	_Binary.is = isArrayBufferLike;
	_Binary.isSource = isArrayBufferSource;
	function fromSource(source) {
		if (ArrayBuffer.isView(source)) return source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength);
		else return source;
	}
	_Binary.fromSource = fromSource;
	function toBase64(source) {
		source = fromSource(source);
		if (typeof Buffer !== "undefined") return Buffer.from(source).toString("base64");
		let binary = "";
		const bytes = new Uint8Array(source);
		for (let i = 0; i < bytes.byteLength; i++) binary += String.fromCharCode(bytes[i]);
		return btoa(binary);
	}
	_Binary.toBase64 = toBase64;
	function fromBase64(source) {
		if (typeof Buffer !== "undefined") return fromSource(Buffer.from(source, "base64"));
		return Uint8Array.from(atob(source), (c) => c.charCodeAt(0));
	}
	_Binary.fromBase64 = fromBase64;
	function toHex(source) {
		source = fromSource(source);
		if (typeof Buffer !== "undefined") return Buffer.from(source).toString("hex");
		return Array.from(new Uint8Array(source), (byte) => byte.toString(16).padStart(2, "0")).join("");
	}
	_Binary.toHex = toHex;
	function fromHex(source) {
		if (typeof Buffer !== "undefined") return fromSource(Buffer.from(source, "hex"));
		const hex = source.length % 2 === 0 ? source : source.slice(0, source.length - 1);
		const buffer = [];
		for (let i = 0; i < hex.length; i += 2) buffer.push(parseInt(`${hex[i]}${hex[i + 1]}`, 16));
		return Uint8Array.from(buffer).buffer;
	}
	_Binary.fromHex = fromHex;
})(Binary || (Binary = {}));
Binary.fromBase64;
Binary.toBase64;
Binary.fromHex;
Binary.toHex;
/** Deep-clone common JavaScript values while preserving prototypes and cycles. */
function clone(source, refs = /* @__PURE__ */ new Map()) {
	if (!source || typeof source !== "object") return source;
	if (is("Date", source)) return new Date(source.valueOf());
	if (is("RegExp", source)) return new RegExp(source.source, source.flags);
	if (isArrayBufferLike(source)) return source.slice(0);
	if (ArrayBuffer.isView(source)) return source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength);
	const cached = refs.get(source);
	if (cached) return cached;
	if (Array.isArray(source)) {
		const result = [];
		refs.set(source, result);
		source.forEach((value, index) => {
			result[index] = Reflect.apply(clone, null, [value, refs]);
		});
		return result;
	}
	const result = Object.create(Object.getPrototypeOf(source));
	refs.set(source, result);
	for (const key of Reflect.ownKeys(source)) {
		const descriptor = { ...Reflect.getOwnPropertyDescriptor(source, key) };
		if ("value" in descriptor) descriptor.value = Reflect.apply(clone, null, [descriptor.value, refs]);
		Reflect.defineProperty(result, key, descriptor);
	}
	return result;
}
/** Deeply compare arrays, dates, regexps, buffers, and plain object fields. */
function deepEqual(a, b, strict) {
	if (a === b) return true;
	if (!strict && isNullable(a) && isNullable(b)) return true;
	if (typeof a !== typeof b) return false;
	if (typeof a !== "object") return false;
	if (!a || !b) return false;
	function check(test, then) {
		return test(a) ? test(b) ? then(a, b) : false : test(b) ? false : void 0;
	}
	return check(Array.isArray, (a, b) => a.length === b.length && a.every((item, index) => deepEqual(item, b[index]))) ?? check(is("Date"), (a, b) => a.valueOf() === b.valueOf()) ?? check(is("RegExp"), (a, b) => a.source === b.source && a.flags === b.flags) ?? check(isArrayBufferLike, (a, b) => {
		if (a.byteLength !== b.byteLength) return false;
		const viewA = new Uint8Array(a);
		const viewB = new Uint8Array(b);
		for (let i = 0; i < viewA.length; i++) if (viewA[i] !== viewB[i]) return false;
		return true;
	}) ?? Object.keys({
		...a,
		...b
	}).every((key) => deepEqual(a[key], b[key], strict));
}
//#endregion
//#region ../../../vendor/cosmokit/src/time.ts
let Time;
(function(_Time) {
	_Time.millisecond = 1;
	const second = _Time.second = 1e3;
	const minute = _Time.minute = second * 60;
	const hour = _Time.hour = minute * 60;
	const day = _Time.day = hour * 24;
	const week = _Time.week = day * 7;
	let timezoneOffset = (/* @__PURE__ */ new Date()).getTimezoneOffset();
	function setTimezoneOffset(offset) {
		timezoneOffset = offset;
	}
	_Time.setTimezoneOffset = setTimezoneOffset;
	function getTimezoneOffset() {
		return timezoneOffset;
	}
	_Time.getTimezoneOffset = getTimezoneOffset;
	function getDateNumber(date = /* @__PURE__ */ new Date(), offset) {
		if (typeof date === "number") date = new Date(date);
		if (offset === void 0) offset = timezoneOffset;
		return Math.floor((date.valueOf() / minute - offset) / 1440);
	}
	_Time.getDateNumber = getDateNumber;
	function fromDateNumber(value, offset) {
		const date = new Date(value * day);
		if (offset === void 0) offset = timezoneOffset;
		return new Date(+date + offset * minute);
	}
	_Time.fromDateNumber = fromDateNumber;
	const numeric = /\d+(?:\.\d+)?/.source;
	const timeRegExp = new RegExp(`^${[
		"w(?:eek(?:s)?)?",
		"d(?:ay(?:s)?)?",
		"h(?:our(?:s)?)?",
		"m(?:in(?:ute)?(?:s)?)?",
		"s(?:ec(?:ond)?(?:s)?)?"
	].map((unit) => `(${numeric}${unit})?`).join("")}$`);
	function parseTime(source) {
		const capture = timeRegExp.exec(source);
		if (!capture) return 0;
		return (parseFloat(capture[1]) * week || 0) + (parseFloat(capture[2]) * day || 0) + (parseFloat(capture[3]) * hour || 0) + (parseFloat(capture[4]) * minute || 0) + (parseFloat(capture[5]) * second || 0);
	}
	_Time.parseTime = parseTime;
	function parseDate(date) {
		const parsed = parseTime(date);
		if (parsed) date = Date.now() + parsed;
		else if (/^\d{1,2}(:\d{1,2}){1,2}$/.test(date)) date = `${(/* @__PURE__ */ new Date()).toLocaleDateString()}-${date}`;
		else if (/^\d{1,2}-\d{1,2}-\d{1,2}(:\d{1,2}){1,2}$/.test(date)) date = `${(/* @__PURE__ */ new Date()).getFullYear()}-${date}`;
		return date ? new Date(date) : /* @__PURE__ */ new Date();
	}
	_Time.parseDate = parseDate;
	function format(ms) {
		const abs = Math.abs(ms);
		if (abs >= day - hour / 2) return Math.round(ms / day) + "d";
		else if (abs >= hour - minute / 2) return Math.round(ms / hour) + "h";
		else if (abs >= minute - second / 2) return Math.round(ms / minute) + "m";
		else if (abs >= second) return Math.round(ms / second) + "s";
		return ms + "ms";
	}
	_Time.format = format;
	function toDigits(source, length = 2) {
		return source.toString().padStart(length, "0");
	}
	_Time.toDigits = toDigits;
	function template(template, time = /* @__PURE__ */ new Date()) {
		return template.replace("yyyy", time.getFullYear().toString()).replace("yy", time.getFullYear().toString().slice(2)).replace("MM", toDigits(time.getMonth() + 1)).replace("dd", toDigits(time.getDate())).replace("hh", toDigits(time.getHours())).replace("mm", toDigits(time.getMinutes())).replace("ss", toDigits(time.getSeconds())).replace("SSS", toDigits(time.getMilliseconds(), 3));
	}
	_Time.template = template;
})(Time || (Time = {}));
//#endregion
//#region ../../../vendor/schemastery/src/index.ts
const kSchema = Symbol.for("schemastery");
const kValidationError = Symbol.for("ValidationError");
globalThis.__schemastery_index__ ??= 0;
globalThis.__schemastery_refs__ = void 0;
var ValidationError = class extends TypeError {
	options;
	name = "ValidationError";
	constructor(message, options) {
		let prefix = "$";
		for (const segment of options.path || []) if (typeof segment === "string") prefix += "." + segment;
		else if (typeof segment === "number") prefix += "[" + segment + "]";
		else if (typeof segment === "symbol") prefix += `[Symbol(${segment.toString()})]`;
		if (prefix.startsWith(".")) prefix = prefix.slice(1);
		super((prefix === "$" ? "" : `${prefix} `) + message);
		this.options = options;
	}
	static is(error) {
		return !!error?.[kValidationError];
	}
};
Object.defineProperty(ValidationError.prototype, kValidationError, { value: true });
const Schema = function(options) {
	const schema = function(data, options = {}) {
		return Schema.resolve(data, schema, options)[0];
	};
	if (options.refs) {
		const refs = mapValues(options.refs, (options) => new Schema(options));
		const getRef = (uid) => refs[uid];
		for (const key in refs) {
			const options = refs[key];
			options.sKey = getRef(options.sKey);
			options.inner = getRef(options.inner);
			options.list = options.list && options.list.map(getRef);
			options.dict = options.dict && mapValues(options.dict, getRef);
		}
		return refs[options.uid];
	}
	Object.assign(schema, options);
	if (typeof schema.callback === "string") try {
		schema.callback = new Function("return " + schema.callback)();
	} catch {}
	Object.defineProperty(schema, "uid", { value: globalThis.__schemastery_index__++ });
	Object.setPrototypeOf(schema, Schema.prototype);
	schema.meta ||= {};
	schema.toString = schema.toString.bind(schema);
	return schema;
};
Schema.prototype = Object.create(Function.prototype);
Schema.prototype[kSchema] = true;
Object.defineProperty(Schema.prototype, "~standard", { get() {
	return {
		version: 1,
		vendor: "schemastery",
		validate: (value) => {
			try {
				return { value: Schema.resolve(value, this, {})[0] };
			} catch (error) {
				if (ValidationError.is(error)) return { issues: [{
					message: error.message,
					path: error.options.path
				}] };
				throw error;
			}
		}
	};
} });
Schema.ValidationError = ValidationError;
Schema.prototype.toJSON = function toJSON() {
	if (globalThis.__schemastery_refs__) {
		globalThis.__schemastery_refs__[this.uid] ??= JSON.parse(JSON.stringify({ ...this }));
		return this.uid;
	}
	globalThis.__schemastery_refs__ = { [this.uid]: { ...this } };
	globalThis.__schemastery_refs__[this.uid] = JSON.parse(JSON.stringify({ ...this }));
	const result = {
		uid: this.uid,
		refs: globalThis.__schemastery_refs__
	};
	globalThis.__schemastery_refs__ = void 0;
	return result;
};
Schema.prototype.set = function set(key, value) {
	this.dict[key] = value;
	return this;
};
Schema.prototype.push = function push(value) {
	this.list.push(value);
	return this;
};
function mergeDesc(original, messages) {
	const result = typeof original === "string" ? { "": original } : { ...original };
	for (const locale in messages) {
		const value = messages[locale];
		if (value?.$description || value?.$desc) result[locale] = value.$description || value.$desc;
		else if (typeof value === "string") result[locale] = value;
	}
	return result;
}
function getInner(value) {
	return value?.$value ?? value?.$inner;
}
function extractKeys(data) {
	return filterKeys(data ?? {}, (key) => !key.startsWith("$"));
}
Schema.prototype.i18n = function i18n(messages) {
	const schema = Schema(this);
	const desc = mergeDesc(schema.meta.description, messages);
	if (Object.keys(desc).length) schema.meta.description = desc;
	if (schema.dict) schema.dict = mapValues(schema.dict, (inner, key) => {
		return inner.i18n(mapValues(messages, (data) => getInner(data)?.[key] ?? data?.[key]));
	});
	if (schema.list) schema.list = schema.list.map((inner, index) => {
		return inner.i18n(mapValues(messages, (data = {}) => {
			if (Array.isArray(getInner(data))) return getInner(data)[index];
			if (Array.isArray(data)) return data[index];
			return extractKeys(data);
		}));
	});
	if (schema.inner) schema.inner = schema.inner.i18n(mapValues(messages, (data) => {
		if (getInner(data)) return getInner(data);
		return extractKeys(data);
	}));
	if (schema.sKey) schema.sKey = schema.sKey.i18n(mapValues(messages, (data) => data?.$key));
	return schema;
};
Schema.prototype.extra = function extra(key, value) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
};
for (const key of [
	"required",
	"disabled",
	"collapse",
	"hidden",
	"loose"
]) Object.assign(Schema.prototype, { [key](value = true) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
} });
Schema.prototype.deprecated = function deprecated() {
	const schema = Schema(this);
	schema.meta.badges ||= [];
	schema.meta.badges.push({
		text: "deprecated",
		type: "danger"
	});
	return schema;
};
Schema.prototype.experimental = function experimental() {
	const schema = Schema(this);
	schema.meta.badges ||= [];
	schema.meta.badges.push({
		text: "experimental",
		type: "warning"
	});
	return schema;
};
Schema.prototype.pattern = function pattern(regexp) {
	const schema = Schema(this);
	const pattern = pick(regexp, ["source", "flags"]);
	schema.meta = {
		...schema.meta,
		pattern
	};
	return schema;
};
Schema.prototype.simplify = function simplify(value) {
	if (deepEqual(value, this.meta.default, this.type === "dict")) return null;
	if (isNullable(value)) return value;
	if (this.type === "object" || this.type === "dict") {
		const result = {};
		for (const key in value) {
			const item = (this.type === "object" ? this.dict[key] : this.inner)?.simplify(value[key]);
			if (this.type === "dict" || !isNullable(item)) result[key] = item;
		}
		if (deepEqual(result, this.meta.default, this.type === "dict")) return null;
		return result;
	} else if (this.type === "array" || this.type === "tuple") {
		const result = [];
		value.forEach((value, index) => {
			const schema = this.type === "array" ? this.inner : this.list[index];
			const item = schema ? schema.simplify(value) : value;
			result.push(item);
		});
		return result;
	} else if (this.type === "intersect") {
		const result = {};
		for (const item of this.list) Object.assign(result, item.simplify(value));
		return result;
	} else if (this.type === "union") for (const schema of this.list) try {
		Schema.resolve(value, schema, {});
		return schema.simplify(value);
	} catch {}
	return value;
};
Schema.prototype.toString = function toString(inline) {
	return formatters[this.type]?.(this, inline) ?? `Schema<${this.type}>`;
};
Schema.prototype.role = function role(role, extra) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		role,
		extra
	};
	return schema;
};
for (const key of [
	"default",
	"link",
	"comment",
	"description",
	"max",
	"min",
	"step"
]) Object.assign(Schema.prototype, { [key](value) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
} });
const resolvers = {};
Schema.extend = function extend(type, resolve) {
	resolvers[type] = resolve;
};
Schema.resolve = function resolve(data, schema, options = {}, strict = false) {
	if (!schema) return [data];
	if (options.ignore?.(data, schema)) return [data];
	if (isNullable(data) && schema.type !== "lazy") {
		if (schema.meta.required) throw new ValidationError(`missing required value`, options);
		let current = schema;
		let fallback = schema.meta.default;
		while (current?.type === "intersect" && isNullable(fallback)) {
			current = current.list[0];
			fallback = current?.meta.default;
		}
		if (isNullable(fallback)) return [data];
		data = clone(fallback);
	}
	const callback = resolvers[schema.type];
	if (!callback) throw new ValidationError(`unsupported type "${schema.type}"`, options);
	try {
		return callback(data, schema, options, strict);
	} catch (error) {
		if (!schema.meta.loose) throw error;
		return [schema.meta.default];
	}
};
Schema.from = function from(source) {
	if (isNullable(source)) return Schema.any();
	else if ([
		"string",
		"number",
		"boolean"
	].includes(typeof source)) return Schema.const(source).required();
	else if (source[kSchema]) return source;
	else if (typeof source === "function") switch (source) {
		case String: return Schema.string().required();
		case Number: return Schema.number().required();
		case Boolean: return Schema.boolean().required();
		case Function: return Schema.function().required();
		default: return Schema.is(source).required();
	}
	else throw new TypeError(`cannot infer schema from ${source}`);
};
Schema.lazy = function lazy(builder) {
	const toJSON = () => {
		if (!schema.inner[kSchema]) {
			schema.inner = schema.builder();
			schema.inner.meta = {
				...schema.meta,
				...schema.inner.meta
			};
		}
		return schema.inner.toJSON();
	};
	const schema = new Schema({
		type: "lazy",
		builder,
		inner: { toJSON }
	});
	return schema;
};
Schema.natural = function natural() {
	return Schema.number().step(1).min(0);
};
Schema.percent = function percent() {
	return Schema.number().step(.01).min(0).max(1).role("slider");
};
Schema.date = function date() {
	return Schema.union([Schema.is(Date), Schema.transform(Schema.string().role("datetime"), (value, options) => {
		const date = new Date(value);
		if (isNaN(+date)) throw new ValidationError(`invalid date "${value}"`, options);
		return date;
	}, true)]);
};
Schema.regExp = function regExp(flag = "") {
	return Schema.union([Schema.is(RegExp), Schema.transform(Schema.string().role("regexp", { flag }), (value, options) => {
		try {
			return new RegExp(value, flag);
		} catch (e) {
			throw new ValidationError(e.message, options);
		}
	}, true)]);
};
Schema.arrayBuffer = function arrayBuffer(encoding) {
	return Schema.union([
		Schema.is(ArrayBuffer),
		Schema.is(SharedArrayBuffer),
		Schema.transform(Schema.any(), (value, options) => {
			if (Binary.isSource(value)) return Binary.fromSource(value);
			throw new ValidationError(`expected ArrayBufferSource but got ${value}`, options);
		}, true),
		...encoding ? [Schema.transform(Schema.string(), (value, options) => {
			try {
				return encoding === "base64" ? Binary.fromBase64(value) : Binary.fromHex(value);
			} catch (e) {
				throw new ValidationError(e.message, options);
			}
		}, true)] : []
	]);
};
Schema.extend("lazy", (data, schema, options, strict) => {
	if (!schema.inner[kSchema]) {
		schema.inner = schema.builder();
		schema.inner.meta = {
			...schema.meta,
			...schema.inner.meta
		};
	}
	return Schema.resolve(data, schema.inner, options, strict);
});
Schema.extend("any", (data) => {
	return [data];
});
Schema.extend("never", (data, _, options) => {
	throw new ValidationError(`expected nullable but got ${data}`, options);
});
Schema.extend("const", (data, { value }, options) => {
	if (deepEqual(data, value)) return [value];
	throw new ValidationError(`expected ${value} but got ${data}`, options);
});
function checkWithinRange(data, meta, description, options, skipMin = false) {
	const { max = Infinity, min = -Infinity } = meta;
	if (data > max) throw new ValidationError(`expected ${description} <= ${max} but got ${data}`, options);
	if (data < min && !skipMin) throw new ValidationError(`expected ${description} >= ${min} but got ${data}`, options);
}
Schema.extend("string", (data, { meta }, options) => {
	if (typeof data !== "string") throw new ValidationError(`expected string but got ${data}`, options);
	if (meta.pattern) {
		const regexp = new RegExp(meta.pattern.source, meta.pattern.flags);
		if (!regexp.test(data)) throw new ValidationError(`expect string to match regexp ${regexp}`, options);
	}
	checkWithinRange(data.length, meta, "string length", options);
	return [data];
});
function decimalShift(data, digits) {
	const str = data.toString();
	if (str.includes("e")) return data * Math.pow(10, digits);
	const index = str.indexOf(".");
	if (index === -1) return data * Math.pow(10, digits);
	const frac = str.slice(index + 1);
	const integer = str.slice(0, index);
	if (frac.length <= digits) return +(integer + frac.padEnd(digits, "0"));
	return +(integer + frac.slice(0, digits) + "." + frac.slice(digits));
}
function isMultipleOf(data, min, step) {
	step = Math.abs(step);
	if (!/^\d+\.\d+$/.test(step.toString())) return (data - min) % step === 0;
	const index = step.toString().indexOf(".");
	const digits = step.toString().slice(index + 1).length;
	return Math.abs(decimalShift(data, digits) - decimalShift(min, digits)) % decimalShift(step, digits) === 0;
}
Schema.extend("number", (data, { meta }, options) => {
	if (typeof data !== "number") throw new ValidationError(`expected number but got ${data}`, options);
	checkWithinRange(data, meta, "number", options);
	const { step } = meta;
	if (step && !isMultipleOf(data, meta.min ?? 0, step)) throw new ValidationError(`expected number multiple of ${step} but got ${data}`, options);
	return [data];
});
Schema.extend("boolean", (data, _, options) => {
	if (typeof data === "boolean") return [data];
	throw new ValidationError(`expected boolean but got ${data}`, options);
});
Schema.extend("bitset", (data, { bits, meta }, options) => {
	let value = 0, keys = [];
	if (typeof data === "number") {
		value = data;
		for (const key in bits) if (data & bits[key]) keys.push(key);
	} else if (Array.isArray(data)) {
		keys = data;
		for (const key of keys) {
			if (typeof key !== "string") throw new ValidationError(`expected string but got ${key}`, options);
			if (key in bits) value |= bits[key];
		}
	} else throw new ValidationError(`expected number or array but got ${data}`, options);
	if (value === meta.default) return [value];
	return [value, keys];
});
Schema.extend("function", (data, _, options) => {
	if (typeof data === "function") return [data];
	throw new ValidationError(`expected function but got ${data}`, options);
});
Schema.extend("is", (data, { constructor }, options) => {
	if (typeof constructor === "function") {
		if (data instanceof constructor) return [data];
		throw new ValidationError(`expected ${constructor.name} but got ${data}`, options);
	} else {
		if (isNullable(data)) throw new ValidationError(`expected ${constructor} but got ${data}`, options);
		let prototype = Object.getPrototypeOf(data);
		while (prototype) {
			if (prototype.constructor?.name === constructor) return [data];
			prototype = Object.getPrototypeOf(prototype);
		}
		throw new ValidationError(`expected ${constructor} but got ${data}`, options);
	}
});
function property(data, key, schema, options) {
	try {
		const [value, adapted] = Schema.resolve(data[key], schema, {
			...options,
			path: [...options.path || [], key]
		});
		if (adapted !== void 0) data[key] = adapted;
		return value;
	} catch (e) {
		if (!options?.autofix) throw e;
		delete data[key];
		return schema.meta.default;
	}
}
Schema.extend("array", (data, { inner, meta }, options) => {
	if (!Array.isArray(data)) throw new ValidationError(`expected array but got ${data}`, options);
	checkWithinRange(data.length, meta, "array length", options, !isNullable(inner.meta.default));
	return [data.map((_, index) => property(data, index, inner, options))];
});
Schema.extend("dict", (data, { inner, sKey }, options, strict) => {
	if (!isPlainObject(data)) throw new ValidationError(`expected object but got ${data}`, options);
	const result = {};
	for (const key in data) {
		let rKey;
		try {
			rKey = Schema.resolve(key, sKey, options)[0];
		} catch (error) {
			if (strict) continue;
			throw error;
		}
		result[rKey] = property(data, key, inner, options);
		data[rKey] = data[key];
		if (key !== rKey) delete data[key];
	}
	return [result];
});
Schema.extend("tuple", (data, { list }, options, strict) => {
	if (!Array.isArray(data)) throw new ValidationError(`expected array but got ${data}`, options);
	const result = list.map((inner, index) => property(data, index, inner, options));
	if (strict) return [result];
	result.push(...data.slice(list.length));
	return [result];
});
function merge(result, data) {
	for (const key in data) {
		if (key in result) continue;
		result[key] = data[key];
	}
}
Schema.extend("object", (data, { dict }, options, strict) => {
	if (!isPlainObject(data)) throw new ValidationError(`expected object but got ${data}`, options);
	const result = {};
	for (const key in dict) {
		const value = property(data, key, dict[key], options);
		if (!isNullable(value) || key in data) result[key] = value;
	}
	if (!strict) merge(result, data);
	return [result];
});
Schema.extend("union", (data, { list, toString }, options, strict) => {
	const messages = [];
	for (const inner of list) try {
		return Schema.resolve(data, inner, options, strict);
	} catch (error) {
		messages.push(error);
	}
	throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
});
Schema.extend("intersect", (data, { list, toString }, options, strict) => {
	if (!list.length) return [data];
	let result;
	for (const inner of list) {
		const value = Schema.resolve(data, inner, options, true)[0];
		if (isNullable(value)) continue;
		if (isNullable(result)) result = value;
		else if (typeof result !== typeof value) throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
		else if (typeof value === "object") merge(result ??= {}, value);
		else if (result !== value) throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
	}
	if (!strict && isPlainObject(data)) merge(result, data);
	return [result];
});
Schema.extend("transform", (data, { inner, callback, preserve }, options) => {
	const [result, adapted = data] = Schema.resolve(data, inner, options, true);
	if (preserve) return [callback(result)];
	else return [callback(result), callback(adapted)];
});
const formatters = {};
function defineMethod(name, keys, format) {
	formatters[name] = format;
	Object.assign(Schema, { [name](...args) {
		const schema = new Schema({ type: name });
		keys.forEach((key, index) => {
			switch (key) {
				case "sKey":
					schema.sKey = args[index] ?? Schema.string();
					break;
				case "inner":
					schema.inner = Schema.from(args[index]);
					break;
				case "list":
					schema.list = args[index].map(Schema.from);
					break;
				case "dict":
					schema.dict = mapValues(args[index], Schema.from);
					break;
				case "bits":
					schema.bits = {};
					for (const key in args[index]) {
						if (typeof args[index][key] !== "number") continue;
						schema.bits[key] = args[index][key];
					}
					break;
				case "callback": {
					const callback = schema.callback = args[index];
					callback["toJSON"] ||= () => callback.toString();
					break;
				}
				case "constructor": {
					const constructor = schema.constructor = args[index];
					if (typeof constructor === "function") constructor["toJSON"] ||= () => constructor["name"];
					break;
				}
				default: schema[key] = args[index];
			}
		});
		if (name === "object" || name === "dict") schema.meta.default = {};
		else if (name === "array" || name === "tuple") schema.meta.default = [];
		else if (name === "bitset") schema.meta.default = 0;
		return schema;
	} });
}
defineMethod("is", ["constructor"], ({ constructor }) => {
	if (typeof constructor === "function") return constructor.name;
	else return constructor;
});
defineMethod("any", [], () => "any");
defineMethod("never", [], () => "never");
defineMethod("const", ["value"], ({ value }) => typeof value === "string" ? JSON.stringify(value) : value);
defineMethod("string", [], () => "string");
defineMethod("number", [], () => "number");
defineMethod("boolean", [], () => "boolean");
defineMethod("bitset", ["bits"], () => "bitset");
defineMethod("function", [], () => "function");
defineMethod("array", ["inner"], ({ inner }) => `${inner.toString(true)}[]`);
defineMethod("dict", ["inner", "sKey"], ({ inner, sKey }) => `{ [key: ${sKey.toString()}]: ${inner.toString()} }`);
defineMethod("tuple", ["list"], ({ list }) => `[${list.map((inner) => inner.toString()).join(", ")}]`);
defineMethod("object", ["dict"], ({ dict }) => {
	if (Object.keys(dict).length === 0) return "{}";
	return `{ ${Object.entries(dict).map(([key, inner]) => {
		return `${key}${inner.meta.required ? "" : "?"}: ${inner.toString()}`;
	}).join(", ")} }`;
});
defineMethod("union", ["list"], ({ list }, inline) => {
	const result = list.map(({ toString: format }) => format()).join(" | ");
	return inline ? `(${result})` : result;
});
defineMethod("intersect", ["list"], ({ list }) => {
	return `${list.map((inner) => inner.toString(true)).join(" & ")}`;
});
defineMethod("transform", [
	"inner",
	"callback",
	"preserve"
], ({ inner }, isInner) => inner.toString(isInner));
//#endregion
//#region lib/types/gateway.js
/**
* QQ 机器人 WebSocket 网关（官方 v2）。
*
* 流程：`GET /gateway` 取 wss 地址 → 连上后收 `op=10 Hello`（带心跳间隔）→
* 发 `op=2 Identify`（`token = "QQBot <access_token>"` + intents + shard）→
* 按间隔发 `op=1 心跳` → 收 `op=0 Dispatch` 业务事件。
*
* 断线处理：`op=7 Reconnect` 或 socket 关闭 → 指数退避重连，优先 `op=6 Resume`
* （带上次的 session_id + seq，平台会补发漏掉的事件）。
*
* @module dsh-webhook-qq/gateway
*/
/** 默认订阅：群 @ + C2C 单聊消息。 */
const DEFAULT_INTENTS = 1 << 25;
/** WebSocket 网关客户端。 */
var QqGateway = class {
	options;
	socket;
	heartbeat;
	seq = 0;
	sessionId = "";
	stopped = false;
	attempt = 0;
	/** 连续被平台踢（op=7）的时间戳；短时间内太多就冷却，避免无限重连战争。 */
	kickTimes = [];
	/** 冷却截止时间（毫秒时间戳）；期间不重连。 */
	cooldownUntil = 0;
	constructor(options) {
		this.options = options;
	}
	/** 启动（立即连接，断线自动重连）。 */
	start() {
		this.stopped = false;
		this.connect();
	}
	/** 停止（不再重连）。 */
	stop() {
		this.stopped = true;
		if (this.heartbeat !== void 0) clearInterval(this.heartbeat);
		this.heartbeat = void 0;
		try {
			this.socket?.close();
		} catch {}
		this.socket = void 0;
	}
	/** 建一次连接；失败就按退避重试。 */
	async connect() {
		if (this.stopped) return;
		const log = this.options.log ?? (() => {});
		try {
			const base = this.options.baseUrl ?? "https://api.bot.qq.com";
			const token = await this.options.token();
			const response = await fetch(`${base}/gateway`, {
				headers: { Authorization: `QQBot ${token}` },
				signal: AbortSignal.timeout(15e3)
			});
			if (!response.ok) throw new Error(`GET /gateway HTTP ${response.status}`);
			const body = await response.json();
			if (typeof body.url !== "string" || body.url === "") throw new Error("GET /gateway 没返回 url");
			const socket = new WebSocket(body.url);
			this.socket = socket;
			socket.addEventListener("open", () => {
				log("gateway 已连接");
			});
			socket.addEventListener("message", (event) => {
				this.handle_frame(typeof event.data === "string" ? event.data : String(event.data));
			});
			socket.addEventListener("close", () => {
				if (this.heartbeat !== void 0) clearInterval(this.heartbeat);
				this.heartbeat = void 0;
				if (this.stopped) return;
				const backoff = Math.min(3e4, 1e3 * 2 ** this.attempt);
				const cooldown = Math.max(0, this.cooldownUntil - Date.now());
				const delay = Math.max(backoff, cooldown);
				this.attempt += 1;
				log(`gateway 断开，${Math.round(delay / 1e3)}s 后重连（第 ${this.attempt} 次）`);
				setTimeout(() => {
					this.connect();
				}, delay);
			});
			socket.addEventListener("error", () => {
				log("gateway 连接错误（等 close 后重连）");
			});
		} catch (error) {
			if (this.stopped) return;
			const delay = Math.min(3e4, 1e3 * 2 ** this.attempt);
			this.attempt += 1;
			log(`gateway 连接失败：${String(error).slice(0, 120)}；${Math.round(delay / 1e3)}s 后重试`);
			setTimeout(() => {
				this.connect();
			}, delay);
		}
	}
	/** 处理一帧：hello / dispatch / 心跳确认 / 重连 / 失效会话。 */
	async handle_frame(raw) {
		const log = this.options.log ?? (() => {});
		let frame;
		try {
			frame = JSON.parse(raw);
		} catch {
			return;
		}
		switch (frame.op) {
			case 10: {
				const interval = Number(frame.d?.heartbeat_interval ?? 3e4);
				this.start_heartbeat(interval);
				await this.identify();
				return;
			}
			case 11: return;
			case 0: {
				if (typeof frame.s === "number") this.seq = frame.s;
				const kind = frame.t;
				if (kind === "READY") {
					const session = frame.d?.session_id;
					if (typeof session === "string") this.sessionId = session;
					this.attempt = 0;
					log("gateway READY");
					return;
				}
				if (kind === "C2C_MESSAGE_CREATE" || kind === "GROUP_AT_MESSAGE_CREATE") this.options.on_event({
					t: kind,
					d: frame.d ?? {}
				});
				return;
			}
			case 7: {
				log("gateway 要求重连（op=7），清空会话后重新 identify");
				this.sessionId = "";
				this.seq = 0;
				const now = Date.now();
				this.kickTimes = this.kickTimes.filter((at) => now - at < 600 * 1e3);
				this.kickTimes.push(now);
				if (this.kickTimes.length >= 5) {
					this.cooldownUntil = now + 300 * 1e3;
					this.kickTimes = [];
					log("gateway 连续被踢 5 次，冷却 5 分钟后再连（避免重连战争）");
				}
				try {
					this.socket?.close();
				} catch {}
				return;
			}
			case 9:
				log("gateway 会话失效（op=9），重新 identify");
				this.sessionId = "";
				this.seq = 0;
				await this.identify();
				return;
			default: return;
		}
	}
	/** 启动心跳（d 带上次的 seq）。 */
	start_heartbeat(interval) {
		if (this.heartbeat !== void 0) clearInterval(this.heartbeat);
		const send = () => {
			try {
				this.socket?.send(JSON.stringify({
					op: 1,
					d: this.seq
				}));
			} catch {}
		};
		send();
		this.heartbeat = setInterval(send, Math.max(5e3, interval));
	}
	/** 发 identify / resume。 */
	async identify() {
		const token = await this.options.token();
		const payload = this.sessionId === "" ? {
			op: 2,
			d: {
				token: `QQBot ${token}`,
				intents: this.options.intents ?? DEFAULT_INTENTS,
				shard: [0, 1],
				properties: {}
			}
		} : {
			op: 6,
			d: {
				token: `QQBot ${token}`,
				session_id: this.sessionId,
				seq: this.seq
			}
		};
		try {
			this.socket?.send(JSON.stringify(payload));
		} catch (error) {
			(this.options.log ?? (() => {}))(`gateway 发送 ${this.sessionId === "" ? "identify" : "resume"} 失败：${String(error).slice(0, 100)}`);
		}
	}
};
//#endregion
//#region lib/types/send.js
/**
* 向 QQ 发送消息（官方 v2 HTTP 接口）。
*
* 两种消息形态（官方定义）：
* - **被动消息**：带 `msg_id`（回复用户那条消息），单聊有效期 **60 分钟 / 每条最多回 4 次**，
*   群聊 **5 分钟 / 最多 5 次**；
* - **主动消息**：不带 `msg_id`，随时可发（受频控：单聊 20 qpm / 1000 条/天/用户）。
*
* 这里默认走"被动回复"（带 msg_id），失败再退回主动推送 —— 这样正常对话零成本，
* 而"没人说话时也想说一句"（定时提醒、长任务完成汇报）时主动推送仍可用。
*
* @module dsh-webhook-qq/send
*/
/** 单条文本上限（官方允许更长，但拆条更好送达；也方便"回想起来"分段）。 */
const CHUNK_CHARS = 1200;
/** 把长文本切成若干条（不拆开英文字母/数字太多的情况，简单按长度切）。 */
function split_text(text, limit = CHUNK_CHARS) {
	const value = text.trim();
	if (value === "") return [];
	const chunks = [];
	for (let index = 0; index < value.length; index += limit) chunks.push(value.slice(index, index + limit));
	return chunks;
}
/**
* 发一条文本消息。
*
* @param tokenProvider - 取 access_token 的函数（过期自动刷新）
* @param target - 单聊或群聊目标
* @param text - 文本内容
* @param msgId - 被动回复时要带的消息 id；省略=主动推送
* @param baseUrl - 接口基址
* @returns 发送结果（不抛异常，便于调用方记录并降级）
*/
async function send_qq_text(tokenProvider, target, text, msgId, baseUrl = "https://api.bot.qq.com") {
	const chunks = split_text(text);
	if (chunks.length === 0) return {
		ok: true,
		mode: msgId === void 0 ? "active" : "passive"
	};
	const scope = target.groupOpenid !== void 0 ? `/v2/groups/${encodeURIComponent(target.groupOpenid)}/messages` : target.userOpenid !== void 0 ? `/v2/users/${encodeURIComponent(target.userOpenid)}/messages` : void 0;
	if (scope === void 0) return {
		ok: false,
		mode: "passive",
		error: "没有发送目标（userOpenid / groupOpenid 都为空）"
	};
	const mode = msgId === void 0 ? "active" : "passive";
	let lastId;
	for (const [index, chunk] of chunks.entries()) {
		const body = {
			content: chunk,
			msg_type: 0
		};
		if (msgId !== void 0) {
			body.msg_id = msgId;
			body.msg_seq = index + 1;
		}
		try {
			const token = await tokenProvider.get();
			const response = await fetch(`${baseUrl}${scope}`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `QQBot ${token}`
				},
				body: JSON.stringify(body),
				signal: AbortSignal.timeout(2e4)
			});
			const raw = await response.text();
			if (!response.ok) return {
				ok: false,
				mode,
				error: `HTTP ${response.status} ${raw.slice(0, 160)}`
			};
			try {
				const parsed = JSON.parse(raw);
				if (typeof parsed.id === "string") lastId = parsed.id;
			} catch {}
		} catch (error) {
			return {
				ok: false,
				mode,
				error: String(error).slice(0, 160)
			};
		}
	}
	return {
		ok: true,
		mode,
		...lastId === void 0 ? {} : { id: lastId }
	};
}
/**
* 按扩展名判断 QQ 媒体类型：1=图片 2=视频 3=语音 4=文件。
*
* 实测（2026-10-27）：mp3 用 file_type=3 发送后，QQ 客户端**正常显示为语音条**，
* 不需要额外转 SILK —— 不要优化成先转码，否则反而多一道依赖。
*/
function media_type_of(path) {
	const dot = path.lastIndexOf(".");
	const ext = dot < 0 ? "" : path.slice(dot + 1).toLowerCase();
	if ([
		"jpg",
		"jpeg",
		"png",
		"gif",
		"webp",
		"bmp"
	].includes(ext)) return 1;
	if ([
		"mp4",
		"mov",
		"avi",
		"mkv",
		"webm"
	].includes(ext)) return 2;
	if ([
		"silk",
		"amr",
		"mp3",
		"wav",
		"ogg",
		"m4a"
	].includes(ext)) return 3;
	return 4;
}
/**
* 发一个本地文件/图片到 QQ（官方富媒体：先上传拿 file_info，再发 msg_type=7）。
*
* 为什么要先上传：QQ 的富媒体消息不带本地路径，必须先调 `/files` 拿到 `file_info`。
* 被动回复优先（带 msg_id）；失败回退主动消息（不带 msg_id）。
*/
async function send_qq_media(tokenProvider, target, filePath, msgId, baseUrl = "https://api.bot.qq.com") {
	const scope = target.groupOpenid !== void 0 ? `/v2/groups/${encodeURIComponent(target.groupOpenid)}` : target.userOpenid !== void 0 ? `/v2/users/${encodeURIComponent(target.userOpenid)}` : void 0;
	if (scope === void 0) return {
		ok: false,
		mode: "passive",
		error: "没有发送目标"
	};
	let data;
	try {
		const { readFile } = await import("node:fs/promises");
		data = await readFile(filePath);
	} catch (error) {
		return {
			ok: false,
			mode: "passive",
			error: `读取文件失败：${String(error).slice(0, 120)}`
		};
	}
	if (data.byteLength > 20 * 1024 * 1024) return {
		ok: false,
		mode: "passive",
		error: `文件过大（${(data.byteLength / 1024 / 1024).toFixed(1)}MB，上限 20MB）`
	};
	try {
		const token = await tokenProvider.get();
		const upload = await fetch(`${baseUrl}${scope}/files`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `QQBot ${token}`
			},
			body: JSON.stringify({
				file_type: media_type_of(filePath),
				file_data: data.toString("base64"),
				srv_send_msg: false
			}),
			signal: AbortSignal.timeout(6e4)
		});
		const uploadRaw = await upload.text();
		if (!upload.ok) return {
			ok: false,
			mode: "passive",
			error: `上传失败 HTTP ${upload.status} ${uploadRaw.slice(0, 160)}`
		};
		const info = JSON.parse(uploadRaw);
		if (typeof info.file_info !== "string" || info.file_info === "") return {
			ok: false,
			mode: "passive",
			error: `上传未返回 file_info：${uploadRaw.slice(0, 160)}`
		};
		const send_once = async (passive) => {
			const body = {
				content: " ",
				msg_type: 7,
				media: { file_info: info.file_info }
			};
			if (passive && msgId !== void 0) {
				body.msg_id = msgId;
				body.msg_seq = 1;
			}
			const response = await fetch(`${baseUrl}${scope}/messages`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `QQBot ${token}`
				},
				body: JSON.stringify(body),
				signal: AbortSignal.timeout(3e4)
			});
			const raw = await response.text();
			const mode = passive && msgId !== void 0 ? "passive" : "active";
			if (!response.ok) return {
				ok: false,
				mode,
				error: `HTTP ${response.status} ${raw.slice(0, 160)}`
			};
			try {
				const parsed = JSON.parse(raw);
				return {
					ok: true,
					mode,
					...parsed.id === void 0 ? {} : { id: parsed.id }
				};
			} catch {
				return {
					ok: true,
					mode
				};
			}
		};
		const first = await send_once(true);
		if (first.ok) return first;
		console.error(`[webhook-qq] 媒体被动发送失败，改主动重试：${first.error ?? ""}`);
		return await send_once(false);
	} catch (error) {
		return {
			ok: false,
			mode: "passive",
			error: String(error).slice(0, 160)
		};
	}
}
//#endregion
//#region lib/types/token.js
/**
* QQ 官方机器人 access_token 提供者。
*
* 官方规则：`POST https://api.bot.qq.com/app/getAppAccessToken`（appId + clientSecret）
* 返回 `{ access_token, expires_in }``，有效期默认 **7200 秒（2 小时）**；
* 有效期内重复获取返回同一个值，过期后必须自己刷新。
*
* 这里做两件事：缓存 + 提前 60 秒刷新（官方**在过期前 60 秒内**取会拿到新 token，
* 旧的这 60 秒仍可用，正好可以平滑过渡）。
*
* @module dsh-webhook-qq/token
*/
/** access_token 提供者：缓存 + 提前刷新。 */
var QqTokenProvider = class {
	options;
	token = "";
	expiresAt = 0;
	pending;
	constructor(options) {
		this.options = options;
	}
	/** 取一个可用 token（必要时刷新；并发调用只发一次请求）。 */
	async get() {
		if (this.token !== "" && Date.now() < this.expiresAt - 6e4) return this.token;
		this.pending ??= this.fetch_token().finally(() => {
			this.pending = void 0;
		});
		return this.pending;
	}
	/** 真正去换 token；失败直接抛（调用方按通道故障处理）。 */
	async fetch_token() {
		const base = this.options.baseUrl ?? "https://api.bot.qq.com";
		const response = await fetch(`${base}/app/getAppAccessToken`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				appId: this.options.appId,
				clientSecret: this.options.clientSecret
			}),
			signal: AbortSignal.timeout(15e3)
		});
		if (!response.ok) throw new Error(`QQ getAppAccessToken HTTP ${response.status}`);
		const data = await response.json();
		if (typeof data.access_token !== "string" || data.access_token === "") throw new Error(`QQ getAppAccessToken failed: ${data.message ?? "no access_token"}`);
		const ttl = Number(data.expires_in ?? 7200);
		this.token = data.access_token;
		this.expiresAt = Date.now() + (Number.isFinite(ttl) && ttl > 0 ? ttl : 7200) * 1e3;
		return this.token;
	}
};
//#endregion
//#region lib/types/index.js
/**
* QQ 官方机器人通道（Cordis 函数式插件）——**与微信通道并存**，互不影响。
*
* 做什么：
* - 用官方 WebSocket 网关收 `C2C_MESSAGE_CREATE`（单聊）/`GROUP_AT_MESSAGE_CREATE`（群 @）；
* - 复用微信那边的 `HarnessSessionHost`（**不改微信包**）：`qq-<openid>` 一个会话，
*   同一套 Agent / 工具 / 记忆 / 人格；
* - 一轮结束后把回复**带回 QQ**：优先"被动回复"（带 msg_id，单聊 60 分钟内最多 4 次），
*   失败自动退回"主动推送"（不受 5 分钟窗口限制，受频控）。
*
* 为什么不复用微信的协调器：那套围绕 iLink 的 delivery/outbox/lease 设计（含发送窗口停放），
* QQ 是官方接口、窗口宽松，硬套反而要改微信代码。这里只借"会话宿主"，其余保持独立 ——
* 这就是"最小修改"。
*
* @module dsh-webhook-qq
*/
/**
* 共享会话 id（微信和 QQ 单聊共用同一段上下文）。
*
* 开关：`DSH_SESSION_SHARED` 非空时启用；具体 id 由微信侧写进
* `$DSH_HOME/shared-session.txt`（清空上下文时它会跟着代次变），
* QQ 每次读文件，保证两边始终指向同一个会话。
* **群聊不参与共享**（`qq-g-...`），否则群里陌生人会和主人混成一段对话。
*/
function shared_session_id() {
	if ((process.env.DSH_SESSION_SHARED ?? "").trim() === "") return "";
	const home = process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh");
	try {
		return readFileSync(join(home, "shared-session.txt"), "utf8").split("\n")[0]?.trim() ?? "";
	} catch {
		return "main";
	}
}
/** Cordis function-plugin 名称。 */
const name = "webhook-qq";
/** 依赖的 Harness 原生服务（缺任一则插件不激活，而不是等第一条消息才炸）。 */
const inject = [
	"agents",
	"agentPresets",
	"sessionPersistence"
];
/** Schemastery 配置定义。 */
const Config = Schema.object({
	app_id: Schema.string().required(),
	client_secret: Schema.string().role("secret").required(),
	workspace_path: Schema.string().required(),
	provider: Schema.string().required(),
	model: Schema.string().required(),
	agent_preset: Schema.string(),
	base_url: Schema.string(),
	intents: Schema.number()
});
/** 注册 QQ 通道（生命周期跟随 Cordis effect）。 */
function apply(ctx, config) {
	if (!config.app_id.trim()) throw new Error("webhook-qq app_id must be non-empty");
	if (!config.client_secret.trim()) throw new Error("webhook-qq client_secret must be non-empty");
	probe("lifecycle", "qq.plugin.apply", {
		app_id: config.app_id,
		preset: config.agent_preset ?? "",
		shared_env: process.env.DSH_SESSION_SHARED ?? "",
		shared_id: shared_session_id()
	});
	const host = new HarnessSessionHost(ctx, {
		...config.agent_preset === void 0 ? {} : { agent_preset: config.agent_preset },
		workspace_path: config.workspace_path,
		provider: config.provider,
		model: config.model
	});
	host.assert_capabilities();
	const tokens = new QqTokenProvider({
		appId: config.app_id,
		clientSecret: config.client_secret,
		...config.base_url === void 0 ? {} : { baseUrl: config.base_url }
	});
	/** 每个会话本轮要发出去的内容。 */
	const pending = /* @__PURE__ */ new Map();
	/** 已经绑定过流监听的会话。 */
	const bound = /* @__PURE__ */ new Set();
	/** 已经注册过 send_file 工具的会话（每个会话只注册一次）。 */
	const media_bound = /* @__PURE__ */ new Set();
	/** 各会话最近一条消息 id（媒体被动回复要用）。 */
	const last_msg = /* @__PURE__ */ new Map();
	/** 已经装过记忆查询工具（timeline 系列 / relation_query）的 agent ctx。 */
	const mem_tools_bound = /* @__PURE__ */ new WeakSet();
	/** 助手回复的落档序号（按会话，保证 id 稳定不重）。 */
	const asst_seq = /* @__PURE__ */ new Map();
	/** 把一条 QQ 消息写进长期记忆时间线（写入由后台链路做，模型只有读工具）。 */
	const record_memory = (session_id, role, text, stable_id) => {
		const suite = get_shared_memory();
		if (suite === void 0) return;
		try {
			suite.timeline.append({
				id: stable_id,
				ts: Date.now(),
				role,
				text,
				session_id,
				user_id: session_id
			});
		} catch (error) {
			probe("memory", "qq.record_failed", { error: String(error) });
		}
	};
	/** 给当前 agent 装只读查询工具（timeline_days/search/read、relation_query）。 */
	const register_query_tools = (ctx) => {
		const suite = get_shared_memory();
		if (suite === void 0) return;
		const target = ctx;
		if (mem_tools_bound.has(target)) return;
		mem_tools_bound.add(target);
		try {
			register_timeline_tools(ctx, suite.timeline);
			register_graph_tools(ctx, suite.graph);
		} catch (error) {
			probe("memory", "qq.register_tools_failed", { error: String(error) });
		}
	};
	/** 一轮结束（或被取消）时把积累的正文发回 QQ。 */
	const flush = async (session_id) => {
		const item = pending.get(session_id);
		pending.delete(session_id);
		if (item === void 0 || item.text.trim() === "") return;
		const seq = (asst_seq.get(session_id) ?? 0) + 1;
		asst_seq.set(session_id, seq);
		record_memory(session_id, "assistant", item.text.trim(), `qq-asst-${session_id}-${seq}`);
		const result = await send_qq_text(tokens, item.target, item.text, item.msgId, config.base_url);
		probe("outbound", "qq.send", {
			session_id,
			mode: result.mode,
			ok: result.ok,
			error: result.error ?? ""
		});
		if (!result.ok && result.mode === "passive") {
			const active = await send_qq_text(tokens, item.target, item.text, void 0, config.base_url);
			probe("outbound", "qq.send_fallback_active", {
				session_id,
				ok: active.ok,
				error: active.error ?? ""
			});
		}
	};
	/** 首次见到某个 agent 时绑定流监听（每个 agent 只绑一次）。 */
	const bind_stream = (agent, session_id) => {
		if (bound.has(session_id)) return;
		bound.add(session_id);
		agent.ctx.on("agent/assistant-stream", (payload) => {
			const frame = payload.frame;
			if (frame?.type !== "chunk") return;
			const chunk = frame.chunk;
			if (chunk?.type !== "text-delta" || typeof chunk.text !== "string") return;
			const item = pending.get(session_id);
			if (item !== void 0) item.text += chunk.text;
		});
		agent.ctx.on("agent/turn-stopping", () => {
			flush(session_id);
		});
	};
	/**
	* 取（或建）会话对应的 Agent。
	*
	* 为什么需要重试：上一轮结束后，**后台压缩**还在写同一个 session 文件，
	* 它会占着写句柄；这时新消息进来 `create/resume` 会抛
	* `SessionAlreadyOwnedError: already owned by an active write handle`。
	* 用户看到的就是"我这边刚才没处理成"。所以这里等一会儿再试（最多 45 秒），
	* 压缩一结束就能拿到 Agent，消息不会丢。
	*/
	const wait_for_agent = async (session_id) => {
		const deadline = Date.now() + 45e3;
		let last_error;
		while (Date.now() < deadline) {
			const live = host.get_agent(session_id);
			if (live !== void 0) return live;
			try {
				return await host.create_agent(session_id);
			} catch (error) {
				last_error = error;
				const message = String(error?.message ?? error);
				if (!/SessionAlreadyOwned|already owned|already exists|正在写入|busy/i.test(message)) throw error;
				probe("inbound", "qq.session_busy", {
					session_id: String(session_id),
					retry_ms: 2e3
				});
				await new Promise((resolve) => setTimeout(resolve, 2e3));
			}
		}
		throw last_error ?? /* @__PURE__ */ new Error("会话一直被占用，等不到写入句柄");
	};
	/** 处理一条入站消息：建/恢复会话 → 注入（运行中则插话）。 */
	const handle_event = async (kind, data) => {
		const author = data.author ?? {};
		const openid = author.user_openid ?? author.member_openid ?? author.id;
		const content = typeof data.content === "string" ? data.content.trim() : "";
		const msg_id = typeof data.id === "string" ? data.id : void 0;
		const group = typeof data.group_openid === "string" ? data.group_openid : void 0;
		if (openid === void 0 || content === "") return;
		const shared = group === void 0 ? shared_session_id() : "";
		const session_id = brandString(shared !== "" ? shared : `qq-${group === void 0 ? openid : `g-${group}-${openid}`}`);
		const target = group === void 0 ? { userOpenid: openid } : { groupOpenid: group };
		probe("inbound", "qq.message", {
			kind,
			session_id: String(session_id),
			chars: content.length,
			group: group !== void 0
		});
		if (msg_id !== void 0) record_memory(String(session_id), "user", content, `qq-${msg_id}`);
		try {
			const agent = await wait_for_agent(session_id);
			register_query_tools(agent.ctx);
			const session_key = String(session_id);
			if (msg_id !== void 0) last_msg.set(session_key, msg_id);
			if (!media_bound.has(session_key)) {
				media_bound.add(session_key);
				agent.ctx.tools.register(defineTool({
					name: "send_file",
					description: "把本地图片/视频/语音/文档真正发到当前 QQ 聊天（不是只给路径）。用户说\"发我\"\"发过来\"\"截图给我\"时用它。",
					parameters: { path: {
						type: "string",
						description: "本地文件绝对路径（20MB 以内）"
					} },
					output: {
						schema: {
							type: "object",
							additionalProperties: false,
							properties: {
								ok: {
									type: "boolean",
									required: true
								},
								text: {
									type: "string",
									required: true
								}
							}
						},
						render: (_args, value) => [{
							type: "text",
							text: value.text
						}]
					},
					async execute(args) {
						const file = String(args.path ?? "").trim();
						if (file === "") return {
							ok: false,
							text: "path 不能为空"
						};
						const result = await send_qq_media(tokens, target, file, last_msg.get(session_key), config.base_url);
						if (result.ok) return {
							ok: true,
							text: `已发送：${file}（${result.mode}）`
						};
						return {
							ok: false,
							text: `发送失败：${result.error ?? "未知错误"}`
						};
					}
				}));
			}
			bind_stream(agent, String(session_id));
			pending.set(String(session_id), {
				text: "",
				...msg_id === void 0 ? {} : { msgId: msg_id },
				target
			});
			const message = createUserMessage({
				content: [{
					type: "text",
					text: content
				}],
				source: {
					kind: "plugin",
					plugin: "webhook-qq",
					form: "notice",
					summary: "QQ 消息"
				}
			});
			if (agent.status === "idle") agent.followup(message);
			else agent.steer(message);
		} catch (error) {
			probe("inbound", "qq.handle_failed", {
				session_id: String(session_id),
				error: String(error).slice(0, 160)
			});
			console.error(`[webhook-qq] 处理消息失败：${String(error).slice(0, 200)}`);
			await send_qq_text(tokens, target, "我这边刚才没处理成，稍后再试一次好吗？", msg_id, config.base_url);
		}
	};
	const gateway = new QqGateway({
		token: () => tokens.get(),
		...config.intents === void 0 ? {} : { intents: config.intents },
		...config.base_url === void 0 ? {} : { baseUrl: config.base_url },
		on_event: (event) => {
			handle_event(event.t, event.d);
		},
		log: (message) => {
			console.error(`[webhook-qq] ${message}`);
			probe("lifecycle", "qq.gateway", { message: message.slice(0, 160) });
		}
	});
	const lock_path = join(homedir(), ".dsh", "qq-gateway.pid");
	const lock_held = (() => {
		try {
			const pid = Number(readFileSync(lock_path, "utf8").trim());
			if (pid > 0 && pid !== process.pid) try {
				process.kill(pid, 0);
				return true;
			} catch {
				return false;
			}
		} catch {
			return false;
		}
		return false;
	})();
	ctx.effect(() => {
		if (lock_held) {
			console.error("[webhook-qq] 另一个实例已占用网关（qq-gateway.pid），本实例不连 QQ");
			probe("lifecycle", "qq.gateway.skip_duplicate", { pid: process.pid });
			return () => {};
		}
		writeFileSync(lock_path, String(process.pid), "utf8");
		gateway.start();
		probe("lifecycle", "qq.gateway.start", {});
		return () => {
			gateway.stop();
			try {
				if (readFileSync(lock_path, "utf8").trim() === String(process.pid)) unlinkSync(lock_path);
			} catch {}
			probe("lifecycle", "qq.gateway.stop", {});
		};
	});
}
//#endregion
export { Config, apply, inject, name };
