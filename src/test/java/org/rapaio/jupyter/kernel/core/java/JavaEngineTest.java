package org.rapaio.jupyter.kernel.core.java;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.rapaio.jupyter.kernel.GeneralProperties;
import org.rapaio.jupyter.kernel.TestUtils;

import jdk.jshell.JShell;
import jdk.jshell.Snippet;
import jdk.jshell.SnippetEvent;
import jdk.jshell.SourceCodeAnalysis;

public class JavaEngineTest {

    @Test
    void buildTest() throws Exception {
        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withTimeoutMillis(-1L)
                .build();

        var result = engine.eval(TestUtils.context(), "int x = 3;int y = 4;y");
        assertNotNull(result);
        assertInstanceOf(Integer.class, result);
        assertEquals(4, result);

        result = engine.eval(TestUtils.context(), "x");
        assertNotNull(result);
        assertInstanceOf(Integer.class, result);
        assertEquals(3, result);
    }

    @Test
    void completionTest() {
        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withTimeoutMillis(-1L)
                .build();
        var matches = engine.complete("Sys", 3);
        assertEquals(0, matches.start());
        assertEquals(3, matches.end());
        assertEquals(1, matches.replacements().size());
        assertEquals("System", matches.replacements().get(0));
    }

    @Test
    void printerTest() throws Exception {
        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withTimeoutMillis(-1L)
                .build();
        var out = engine.eval(TestUtils.context(), "System.out.println(\"test\")");
        assertNull(out);
    }

    @Test
    void testSnippetDependence() {
        JavaEngine javaEngine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withTimeoutMillis(-1L)
                .build();

        JShell shell = javaEngine.getShell();

        String[] sourceSnippets = new String[] {
                "import java.util.*;",
                "List<Integer> l = new ArrayList<>();",
                "System.out.println(l);",
                "l.add(7);",
                "var x = l.get(0)",
                "System.out.println(l);",
                "l.add(1)"
        };

        List<Snippet> snippets = new ArrayList<>();

        for (String sourceSnippet : sourceSnippets) {

            String code = sourceSnippet;

            while (true) {
                SourceCodeAnalysis.CompletionInfo ci = shell.sourceCodeAnalysis().analyzeCompletion(code);

                if (ci.completeness().isComplete()) {
                    System.out.println("Snippet: " + ci.source());
                    List<SnippetEvent> snippetEvents = shell.eval(ci.source());
                    for (var event : snippetEvents) {
                        snippets.add(event.snippet());
                        System.out.println(event);
                        if (event.exception() != null) {
                            System.out.println(event.exception().getMessage());
                        }
                    }
                    String remaining = ci.remaining();
                    if (remaining == null || remaining.isEmpty()) {
                        break;
                    }
                    code = ci.remaining();
                } else {
                    System.out.println("Incomplete snippet: " + ci.source());
                    break;
                }
            }
        }

        for (int i = 0; i < snippets.size(); i++) {
            System.out.println(i + " " + snippets.get(i).toString());
        }

        List<SnippetEvent> events = shell.drop(snippets.get(1));
        for (var event : events) {
            System.out.println(event);
        }
    }

    @Test
    void testJavaEngineBuilder() throws Exception {
        String compilerOptions = GeneralProperties.defaultProperties().getDefaultCompilerOptions();

        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withCompilerOptions(Arrays.asList(compilerOptions.split(" ")))
                .build();
        engine.initialize();
        engine.eval(TestUtils.context(), "");
    }

    @Test
    void outputOnAssignmentTest() throws Exception {
        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole())
                .withTimeoutMillis(-1L)
                .build();

        var result = engine.eval(TestUtils.context(), "10");
        assertNotNull(result);
        assertEquals("10", result.toString());

        result = engine.eval(TestUtils.context(), "int x = 1;");
        assertNull(result);

        result = engine.eval(TestUtils.context(), "x");
        assertNotNull(result);
        assertEquals("1", result.toString());

        result = engine.eval(TestUtils.context(), "int[] y = new int[10];");
        assertNull(result);

        result = engine.eval(TestUtils.context(), "y[0] = 1;");
        // TODO: it would be nice to isolate that and output nothing
        //assertNull(result);
        assertNotNull(result);

        result = engine.eval(TestUtils.context(), "int[] x = new int[1]; x[0] = 3;");
        assertNotNull(result);
    }
}
