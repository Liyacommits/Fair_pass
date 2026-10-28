package com.liyacommits.fair_pass.controller;


import com.liyacommits.fair_pass.response.TicketResponse;
import com.liyacommits.fair_pass.service.TicketService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/v1")
public class TicketController {

    private TicketService ticketService;

    @Autowired
    public TicketController(TicketService ticketService) {
        this.ticketService = ticketService;
    }


    @GetMapping("ticket/events")
    public @ResponseBody List<TicketResponse> getAllEvents() {
        return TicketService.getAllEvents();
    }
}
