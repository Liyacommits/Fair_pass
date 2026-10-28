package com.liyacommits.fair_pass.service;


import com.liyacommits.fair_pass.repository.EventRepository;
import com.liyacommits.fair_pass.repository.VenueRepository;
import com.liyacommits.fair_pass.response.TicketResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TicketService {

    private final EventRepository eventRepository;
    private final VenueRepository venueRepository;

    @Autowired
    public TicketService(final EventRepository eventRepository,final VenueRepository venueRepository){
        this.eventRepository = eventRepository;
        this.venueRepository = venueRepository;
    }

    public List<TicketResponse> getAllEvents() {

    }
}
