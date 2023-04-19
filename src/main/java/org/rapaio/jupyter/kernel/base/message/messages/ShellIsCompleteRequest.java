package org.rapaio.jupyter.kernel.base.message.messages;

import org.rapaio.jupyter.kernel.base.message.ContentType;
import org.rapaio.jupyter.kernel.base.message.MessageType;

public record ShellIsCompleteRequest(String code) implements ContentType<ShellIsCompleteRequest> {

    @Override
    public MessageType<ShellIsCompleteRequest> type() {
        return MessageType.SHELL_IS_COMPLETE_REQUEST;
    }
}
