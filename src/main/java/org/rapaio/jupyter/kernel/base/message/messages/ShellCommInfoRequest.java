package org.rapaio.jupyter.kernel.base.message.messages;

import org.rapaio.jupyter.kernel.base.message.ContentType;
import org.rapaio.jupyter.kernel.base.message.MessageType;

import com.google.gson.annotations.SerializedName;

public record ShellCommInfoRequest(
        @SerializedName("target_name") String targetName) implements ContentType<ShellCommInfoRequest> {

    @Override
    public MessageType<ShellCommInfoRequest> type() {
        return MessageType.SHELL_COMM_INFO_REQUEST;
    }
}
