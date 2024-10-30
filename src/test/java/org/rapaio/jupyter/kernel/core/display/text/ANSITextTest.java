package org.rapaio.jupyter.kernel.core.display.text;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.rapaio.jupyter.kernel.TestUtils;
import org.rapaio.jupyter.kernel.core.ExecutionContext;
import org.rapaio.jupyter.kernel.core.java.CompilerException;
import org.rapaio.jupyter.kernel.core.java.JavaEngine;

import jdk.jshell.DeclarationSnippet;
import jdk.jshell.Snippet;
import jdk.jshell.SnippetEvent;

public class ANSITextTest {

    @Test
    void sandboxTest() {
        String code = """
            for(int i=0; i<10; i++) {
                y=12;
            }
            """;
        JavaEngine engine = JavaEngine.builder(TestUtils.getTestJShellConsole(), new ExecutionContext(null)).build();
        CompilerException ce = assertThrows(CompilerException.class, () -> engine.eval(TestUtils.context(), code));

        List<String> msgs = new ArrayList<>();
        SnippetEvent event = ce.badSnippet();
        Snippet snippet = event.snippet();
        var diagnostics = engine.getShell().diagnostics(snippet).toList();
        for (var d : diagnostics) {
            msgs.addAll(ANSI.sourceCode(snippet.source(), (int) d.getPosition(),
                    (int) d.getStartPosition(), (int) d.getEndPosition()));

            msgs.addAll(ANSI.errorMessages(d.getMessage(Locale.getDefault())));
            msgs.add("");
        }
        // Declaration snippets are unique in that they can be active with unresolved references
        if (snippet instanceof DeclarationSnippet declarationSnippet) {
            List<String> unresolvedDependencies = engine.getShell().unresolvedDependencies(declarationSnippet).toList();
            if (!unresolvedDependencies.isEmpty()) {
                msgs.addAll(ANSI.sourceCode(snippet.source(), -1, -1, -1));
                msgs.addAll(ANSI.errorMessages("Unresolved dependencies:"));
                unresolvedDependencies.forEach(dep -> msgs.addAll(ANSI.errorMessages("   - " + dep)));
            }
        }

        for(var msg : msgs) {
            System.out.println(msg);
        }

    }
}
