# Style Standards

## General Code Shape

- When two implementations are functionally equivalent and equally clear, prefer
  the one that produces less code. Do not add indirection, helpers, or ceremony
  unless it improves safety, readability, reuse, or testability.

## Apex Null Handling

- Prefer `?.` and `??` over verbose ternaries or nested null checks.

Bad:

```apex
String id = recordId == null ? null : recordId.toString();
String name = value == null ? fallback : value;
```

Good:

```apex
String id = recordId?.toString();
String name = value ?? fallback;
```

- Do not add defensive null branches for values already guaranteed non-null by
  the surrounding condition, API contract, or runtime context.

Bad:

```apex
if (date != null && now > date) {}
```

Good:

```apex
if (now > date) {}
```

## Apex Coercion

- Prefer natural string coercion in concatenation.

Bad:

```apex
'job:' + String.valueOf(System.now().getTime());
```

Good:

```apex
'job:' + System.now().getTime();
```

- For string interpolation in messages, names, or keys:
    - Use concatenation for zero or one dynamic value.
    - Use `String.format(...)` for two or more dynamic values.
    - Prefer typed format arguments, or `List<Object>` for mixed types. Do not wrap
      format arguments in `String.valueOf(...)` when they can be formatted
      directly.
    - Static SOQL fragments and field lists may use concatenation when that keeps
      the query readable.
- Exception message templates belong in `private static final` constants even
  when they have zero or one dynamic value. This keeps a class's failure surface
  visible at the top. Local non-exception strings may stay inline.

Good:

```apex
'Value "' + value + '" is invalid.';
```

Bad:

```apex
'Value "' + value + '" for field "' + fieldName + '" is invalid.';
```

Good:

```apex
String.format('Value "{0}" for field "{1}" is invalid.', new List<Object>{ value, fieldName });
```

Good:

```apex
private static final String INVALID_VALUE_MESSAGE = 'Value "{0}" is invalid.';

throw new QueueException(String.format(INVALID_VALUE_MESSAGE, new List<String>{ value }));
```

- Prefer `Map<Id, ...>` when the key is naturally a Salesforce id.

Bad:

```apex
Map<String, Job__c> jobsById;
jobsById.put(job.Id.toString(), job);
```

Good:

```apex
Map<Id, Job__c> jobsById;
jobsById.put(job.Id, job);
```

- `Id` values are string-compatible in Apex comparisons. Do not cast or
  stringify an `Id` only to compare it with a `String`.

Bad:

```apex
job.Id.toString() == jobId;
```

Good:

```apex
job.Id == jobId;
```

## Impossible Cases

- Prefer sets when comparing one value against 3+ candidates.

Bad:

```apex
val == A || val == B || val == C;
```

Good:

```apex
values.contains(val);
```

## Apex Control Flow

- Always prefer early returns.
- If an `if` guard owns more than half of a method, invert the guard and return
  early.

Bad:

```apex
if (valid) {
    run();
    return result;
}
return fallback;
```

Good:

```apex
if (!valid) {
    return fallback;
}
run();
return result;
```

- Never cover impossible cases just to appear defensive or increase coverage.

Bad:

```apex
ctx?.getTriggerId();
```

Good:

```apex
ctx.getTriggerId();
```

## Apex Class Layout

- Keep class elements in this order:

1. public static final constants
2. public static fields
3. public final fields
4. public fields
5. protected final fields
6. protected fields
7. private static final constants
8. private static fields
9. private final fields
10. private fields
11. public accessors
12. private accessors
13. public constructors
14. protected constructors
15. private constructors
16. public static methods
17. public abstract methods
18. public virtual methods
19. public non-virtual methods
20. protected abstract methods
21. private static methods
22. private virtual methods
23. private non-virtual methods
24. public enums
25. public inner interfaces
26. public abstract classes
27. public classes
28. private enums
29. private inner interfaces
30. private abstract classes
31. private classes

- If a class has both public and private methods, separate them with named
  sections. Prefer specific public section names such as `Queries`, `DMLs`,
  `Accessors`, or `Commands`. Use `API` only when no clearer name fits.
  Private methods belong in `Helpers`.

- Repository classes must use `inherited sharing`. Access levels control
  CRUD/FLS enforcement; they do not replace the repository sharing boundary.

```apex
// <editor-fold desc="Queries">
// </editor-fold>

// <editor-fold desc="Helpers">
// </editor-fold>
```
