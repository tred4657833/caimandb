package com.caimandb.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A tiny, dependency-free JSON encoder/decoder used internally by
 * {@link CaimanDBClient}. It supports exactly the shapes CaimanDB's
 * HTTP API sends and receives: objects ({@code Map<String, Object>}),
 * arrays ({@code List<Object>}), strings, numbers (as {@code Long} or
 * {@code Double}), booleans, and {@code null}.
 *
 * mapping) -- it exists so this driver has zero external dependencies.
 * If your project already depends on Jackson/Gson, feel free to swap
 * this out.
 */
final class Json {

    private Json() {
    }

    // ---- Encoding -----------------------------------------------------

    static String encode(Object value) {
        StringBuilder sb = new StringBuilder();
        encodeValue(value, sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void encodeValue(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            encodeString((String) value, sb);
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value.toString());
        } else if (value instanceof Map) {
            encodeMap((Map<String, Object>) value, sb);
        } else if (value instanceof List) {
            encodeList((List<Object>) value, sb);
        } else if (value instanceof Object[]) {
            encodeList(java.util.Arrays.asList((Object[]) value), sb);
        } else {
            // Fallback: treat unknown types as their string form.
            encodeString(value.toString(), sb);
        }
    }

    private static void encodeMap(Map<String, Object> map, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            encodeString(entry.getKey(), sb);
            sb.append(':');
            encodeValue(entry.getValue(), sb);
        }
        sb.append('}');
    }

    private static void encodeList(List<Object> list, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for (Object item : list) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            encodeValue(item, sb);
        }
        sb.append(']');
    }

    private static void encodeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    // ---- Decoding -----------------------------------------------------

    static Object decode(String text) {
        Parser parser = new Parser(text);
        Object value = parser.parseValue();
        parser.skipWhitespace();
        return value;
    }

    private static final class Parser {
        private final String text;
        private int pos;

        Parser(String text) {
            this.text = text;
            this.pos = 0;
        }

        void skipWhitespace() {
            while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
                pos++;
            }
        }

        char peek() {
            return text.charAt(pos);
        }

        Object parseValue() {
            skipWhitespace();
            char c = peek();
            if (c == '{') {
                return parseObject();
            } else if (c == '[') {
                return parseArray();
            } else if (c == '"') {
                return parseString();
            } else if (c == 't' || c == 'f') {
                return parseBoolean();
            } else if (c == 'n') {
                pos += 4; // "null"
                return null;
            } else {
                return parseNumber();
            }
        }

        Map<String, Object> parseObject() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++; // consume '{'
            skipWhitespace();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                pos++; // consume ':'
                Object value = parseValue();
                map.put(key, value);
                skipWhitespace();
                char c = peek();
                if (c == ',') {
                    pos++;
                    continue;
                } else if (c == '}') {
                    pos++;
                    break;
                } else {
                    throw new IllegalArgumentException("malformed JSON object at position " + pos);
                }
            }
            return map;
        }

        List<Object> parseArray() {
            List<Object> list = new ArrayList<>();
            pos++; // consume '['
            skipWhitespace();
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                Object value = parseValue();
                list.add(value);
                skipWhitespace();
                char c = peek();
                if (c == ',') {
                    pos++;
                    continue;
                } else if (c == ']') {
                    pos++;
                    break;
                } else {
                    throw new IllegalArgumentException("malformed JSON array at position " + pos);
                }
            }
            return list;
        }

        String parseString() {
            pos++; // consume opening '"'
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = text.charAt(pos++);
                if (c == '"') {
                    break;
                }
                if (c == '\\') {
                    char esc = text.charAt(pos++);
                    switch (esc) {
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'u':
                            String hex = text.substring(pos, pos + 4);
                            sb.append((char) Integer.parseInt(hex, 16));
                            pos += 4;
                            break;
                        default:
                            sb.append(esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        Boolean parseBoolean() {
            if (peek() == 't') {
                pos += 4; // "true"
                return Boolean.TRUE;
            }
            pos += 5; // "false"
            return Boolean.FALSE;
        }

        Object parseNumber() {
            int start = pos;
            boolean isFloat = false;
            while (pos < text.length()) {
                char c = text.charAt(pos);
                if (c == '-' || c == '+' || (c >= '0' && c <= '9')) {
                    pos++;
                } else if (c == '.' || c == 'e' || c == 'E') {
                    isFloat = true;
                    pos++;
                } else {
                    break;
                }
            }
            String numStr = text.substring(start, pos);
            if (isFloat) {
                return Double.parseDouble(numStr);
            }
            try {
                return Long.parseLong(numStr);
            } catch (NumberFormatException e) {
                return Double.parseDouble(numStr);
            }
        }
    }
}
