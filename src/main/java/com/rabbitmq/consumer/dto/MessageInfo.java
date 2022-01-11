package com.rabbitmq.consumer.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class MessageInfo {
    private String publisher;
    private String message;
}
