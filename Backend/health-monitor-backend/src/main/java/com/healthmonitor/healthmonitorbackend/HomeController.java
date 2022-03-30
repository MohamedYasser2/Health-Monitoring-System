package com.healthmonitor.healthmonitorbackend;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;

@RestController
@CrossOrigin
public class HomeController {
    @GetMapping("/get/statistics/")
    public ArrayList<String> getStatistics(@RequestParam String startDate, @RequestParam String endDate) {
        System.out.println("I am hereeeeeee");
        return new ArrayList<>();
    }
}